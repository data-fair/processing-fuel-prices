const download = require('./src/download')
const processData = require('./src/process')
// const upload = require('./src/upload')

const baseDataset = {
  isRest: true,
  description: 'une description',
  origin: 'https://donnees.roulez-eco.fr/opendata/',
  license: {
    title: 'Licence Ouverte / Open Licence',
    href: 'https://www.etalab.gouv.fr/licence-ouverte-open-licence'
  },
  schema: require('./src/schema.json'),
  primaryKey: ['id', 'type_carburant'],
  rest: {
    history: true,
    historyTTL: {
      value: 30,
      unit: 'days'
    }
  }
}

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  await download(pluginConfig, tmpDir, axios, log)
  const body = {
    ...baseDataset,
    title: processingConfig.dataset.title
    // extras: { processingId }
  }

  let dataset
  if (processingConfig.datasetMode === 'create') {
    if (processingConfig.dataset.id) {
      try {
        await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)
        throw new Error('le jeu de données existe déjà')
      } catch (err) {
        if (err.status !== 404) throw err
      }
      // permet de créer le jeu de donnée éditable avec l'identifiant spécifié
      dataset = (await axios.put('api/v1/datasets/' + processingConfig.dataset.id, body)).data
    } else {
      // si aucun identifiant n'est spécifié, on créer le dataset juste à partir de son nom
      dataset = (await axios.post('api/v1/datasets', body)).data
    }
    await log.info(`jeu de donnée créé, id="${dataset.id}", title="${dataset.title}"`)
  } else if (processingConfig.datasetMode === 'update') {
    // permet de vérifier l'existance du jeu de donnée avant de réaliser des opérations dessus
    await log.step('Vérification du jeu de données')
    dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
    if (!dataset) throw new Error(`le jeu de données n'existe pas, id${processingConfig.dataset.id}`)
    await log.info(`le jeu de donnée existe, id="${dataset.id}", title="${dataset.title}"`)
  }

  const bulk = await processData(tmpDir, log)

  await log.info(`envoi de ${bulk.length} lignes vers le jeu de données`)
  while (bulk.length) {
    // if (_stopped) return await log.info('interruption demandée')
    const lines = bulk.splice(0, 1000)
    const res = await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, lines)
    if (res.data.nbErrors) {
      log.error(`${res.data.nbErrors} échecs sur ${lines.length} lignes à insérer`, res.data.errors)
      throw new Error('échec à l\'insertion des lignes dans le jeu de données')
    }
  }
}
