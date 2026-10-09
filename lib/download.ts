import fs from 'fs-extra'
import path from 'path'
import { promisify } from 'util'
import { execFile as execFileCb } from 'child_process'
import { pipeline } from 'stream/promises'
import type { AxiosInstance } from 'axios'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'

const execFile = promisify(execFileCb)

export const SOURCE_URL = 'https://donnees.roulez-eco.fr/opendata/instantane'
export const XML_FILE = 'carburants.xml'

export default async (tmpDir: string, axios: AxiosInstance, log: ProcessingContext<ProcessingConfig>['log']): Promise<void> => {
  await log.step('Téléchargement du fichier instantané')
  const file = path.join(tmpDir, 'instantane.zip')

  // creating empty file before streaming seems to fix some weird bugs with NFS
  await fs.ensureFile(file)
  await log.info('Télécharge le fichier ' + SOURCE_URL)
  // the worker axios refuses redirects by default, an external host may use some
  const res = await axios.get(SOURCE_URL, { responseType: 'stream', maxRedirects: 5, timeout: 5 * 60 * 1000 })
  await pipeline(res.data, fs.createWriteStream(file))

  // Try to prevent weird bug with NFS by forcing syncing file before reading it
  const fd = await fs.open(file, 'r')
  await fs.fsync(fd)
  await fs.close(fd)

  await log.info('Extraction de l\'archive')
  try {
    await execFile('unzip', ['-o', file, '-d', tmpDir])
  } catch (err) {
    // unzip exits with a non-zero code on simple warnings, a missing xml is caught below
    await log.warning('Avertissement à l\'extraction de l\'archive', err instanceof Error ? err.message : err)
  }
  const xmlFile = (await fs.readdir(tmpDir)).find(f => f.endsWith('.xml') && f.toUpperCase().includes('CARBURANT'))
  if (!xmlFile) throw new Error('aucun fichier XML de prix des carburants dans l\'archive')
  await fs.rename(path.join(tmpDir, xmlFile), path.join(tmpDir, XML_FILE))
  await fs.remove(file)
}
