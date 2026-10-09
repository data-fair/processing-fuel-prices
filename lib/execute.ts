import type { AxiosInstance } from 'axios'
import type { RunFunction, ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import download, { SOURCE_URL } from './download.ts'
import processData, { type BulkLine } from './process.ts'
import schema from './schema.json' with { type: 'json' }

type Log = ProcessingContext<ProcessingConfig>['log']
type BulkError = { line: number, status: number, error?: string }

let shouldBeStopped = false
export const stop = async (): Promise<void> => { shouldBeStopped = true }

const baseDataset = {
  isRest: true,
  description: 'Ces données sont actualisées sur notre plateforme toutes les 3h. Elles proviennent d\'un traitement des données mises à disposition à partir du système d\'information "Prix Carburants " du Ministère de l\'économie, des finances et de la relance.\n\nIl y a un peu moins de 10 000 stations distinctes, mais dans le format que nous mettons à disposition, il y a une ligne par type de carburant par station. Le format des horaires d\'ouverture est [celui décrit sur schema.org](https://schema.org/openingHours).',
  origin: SOURCE_URL,
  license: {
    title: 'Licence Ouverte / Open Licence',
    href: 'https://www.etalab.gouv.fr/licence-ouverte-open-licence'
  },
  schema,
  primaryKey: ['id', 'type_carburant'],
  rest: {
    history: true,
    historyTTL: { active: true, delay: { value: 30, unit: 'days' } }
  }
}

export const BATCH_SIZE = 1000
export const MAX_ATTEMPTS = 3
export const retryDelay = { ms: 10000 }

// 429 and 5xx are overloads (e.g. an elasticsearch circuit breaker), worth another try
const isTransient = (status?: number) => status === 429 || (status !== undefined && status >= 500)

/**
 * Send one batch of lines, retrying transient failures. Every action is idempotent
 * (createOrUpdate, delete with tolerated 404) so a batch can safely be sent again.
 * Returns the number of lines to delete that were already absent.
 */
export const sendBatch = async (axios: AxiosInstance, log: Log, datasetId: string, lines: BulkLine[]): Promise<number> => {
  for (let attempt = 1; ; attempt++) {
    let data: { nbErrors?: number, errors?: BulkError[] }
    try {
      data = (await axios.post(`api/v1/datasets/${datasetId}/_bulk_lines`, lines)).data
    } catch (err: any) {
      const status = err.status ?? err.response?.status
      if (attempt >= MAX_ATTEMPTS || !isTransient(status)) throw err
      await log.warning(`erreur ${status} à l'envoi des lignes, nouvel essai dans ${retryDelay.ms / 1000}s`)
      await new Promise(resolve => setTimeout(resolve, retryDelay.ms))
      continue
    }
    if (!data.nbErrors) return 0

    const errors = data.errors ?? []
    // a line we ask to delete that is already gone is not a failure, the target state is reached
    const isAlreadyDeleted = (err: BulkError) => err.status === 404 && lines[err.line]?._action === 'delete'
    // errors we cannot attribute (the API caps the detailed list at 50) are never ignored
    const allListed = errors.length === data.nbErrors
    if (allListed && errors.every(isAlreadyDeleted)) return data.nbErrors
    if (attempt < MAX_ATTEMPTS && errors.length && errors.every(err => isAlreadyDeleted(err) || isTransient(err.status))) {
      await log.warning(`${data.nbErrors} échecs temporaires sur ${lines.length} lignes, nouvel essai dans ${retryDelay.ms / 1000}s`, errors)
      await new Promise(resolve => setTimeout(resolve, retryDelay.ms))
      continue
    }
    await log.error(`${data.nbErrors} échecs sur ${lines.length} lignes à insérer`, errors)
    throw new Error('échec à l\'insertion des lignes dans le jeu de données')
  }
}

export const run: RunFunction<ProcessingConfig> = async (context) => {
  const { processingConfig, tmpDir, axios, log, patchConfig, processingId } = context
  shouldBeStopped = false
  // patchConfig mutates processingConfig, read the mode before switching it to update
  const creating = processingConfig.datasetMode === 'create'

  let datasetId: string
  if (creating) {
    await log.step('Création du jeu de donnée')
    const dataset = (await axios.post('api/v1/datasets', {
      ...baseDataset,
      title: processingConfig.datasetTitle || 'Prix des carburants',
      extras: { processingId }
    })).data
    await log.info(`jeu de donnée créé, id="${dataset.id}", title="${dataset.title}"`)
    // switch to update right away, a later failure must not create a second dataset,
    // and until a first full send the next run must reconsider every line
    await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title }, lastSync: new Date(0).toISOString() } as any)
    datasetId = dataset.id
  } else {
    await log.step('Vérification du jeu de données')
    const dataset = (await axios.get(`api/v1/datasets/${(processingConfig.dataset as { id: string }).id}`)).data
    await log.info(`le jeu de donnée existe, id="${dataset.id}", title="${dataset.title}"`)
    datasetId = dataset.id
  }

  await download(tmpDir, axios, log)
  if (shouldBeStopped) return
  const bulk = await processData(creating ? { datasetMode: 'create' } : processingConfig, tmpDir, axios, log)

  // bulk is undefined when there is no line to update
  if (bulk !== undefined) {
    await log.info(`envoi de ${bulk.length} lignes vers le jeu de données`)
    // newest source date in this run, saved only once every line is sent (read back in process.ts)
    const lastSync = bulk.reduce<string | undefined>((max, line) => line.maj_carburant && new Date(line.maj_carburant) > new Date(max || 0) ? line.maj_carburant : max, undefined)
    let nbAlreadyDeleted = 0
    for (let i = 0; i < bulk.length; i += BATCH_SIZE) {
      // lastSync is not saved below, the next run sends the remaining lines again
      if (shouldBeStopped) return await log.warning('Traitement interrompu, les lignes restantes seront envoyées à la prochaine exécution')
      nbAlreadyDeleted += await sendBatch(axios, log, datasetId, bulk.slice(i, i + BATCH_SIZE))
    }
    if (nbAlreadyDeleted) {
      await log.warning(`${nbAlreadyDeleted} ligne(s) à supprimer étaient déjà absentes du jeu de données`)
    }
    if (lastSync) await patchConfig({ lastSync: new Date(lastSync).toISOString() } as any)
  }
}
