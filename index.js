const download = require('./src/download')
const processData = require('./src/process')
const fs = require('fs-extra')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  await download(pluginConfig, tmpDir, axios, log)

  const baseDataset = {
    isRest: true,
    description: 'Ces données sont actualisées sur notre plateforme toutes les 3h. Elles proviennent d\'un traitement des données mises à disposition à partir du système d\'information \"Prix Carburants \" du Ministère de l\'économie, des finances et de la relance.\n\nIl y a un peu moins de 10 000 stations distinctes, mais dans le format que nous mettons à disposition, il y a une ligne par type de carburant par station. Le format des horaires d\'ouverture est [celui décrit sur schema.org](https://schema.org/openingHours).',
    origin: pluginConfig.url,
    license: {
      title: 'Licence Ouverte / Open Licence',
      href: 'https://www.etalab.gouv.fr/licence-ouverte-open-licence'
    },
    schema: require('./src/schema.json'),
    primaryKey: ['id', 'type_carburant'],
    rest: {
      history: true,
      historyTTL: {
        active: true,
        delay: {
          value: 30,
          unit: 'days'
        }
      }
    }
  }

  const body = {
    ...baseDataset,
    title: processingConfig.dataset.title
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
      dataset = (await axios.put('api/v1/datasets/' + processingConfig.dataset.id, body)).data
    } else {
      dataset = (await axios.post('api/v1/datasets', body)).data
    }
    await log.info(`jeu de donnée créé, id="${dataset.id}", title="${dataset.title}"`)
  } else if (processingConfig.datasetMode === 'update') {
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

  if (processingConfig.clearFiles) {
    await fs.emptyDir('./')
  }
}
