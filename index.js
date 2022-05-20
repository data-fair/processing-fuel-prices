const download = require('./src/download')
const processData = require('./src/process')
const fs = require('fs-extra')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
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
  // check the current state of the dataset
  if (processingConfig.datasetMode === 'create') {
    await log.step('Création du jeu de donnée')
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
    try {
      dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
      await log.info(`le jeu de donnée existe, id="${dataset.id}", title="${dataset.title}"`)
    } catch (err) {
      if (!dataset) throw new Error(`le jeu de données n'existe pas, id ${processingConfig.dataset.id}`)
    }
  }

  await download(pluginConfig, tmpDir, axios, log)
  const bulk = await processData(processingConfig, tmpDir, axios, log)

  // bulk is undefined when there is no line to update
  if (bulk !== undefined) {
    await log.info(`envoi de ${bulk.length} lignes vers le jeu de données`)
    while (bulk.length) {
      const lines = bulk.splice(0, 1000)
      const res = await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, lines)
      if (res.data.nbErrors) {
        log.error(`${res.data.nbErrors} échecs sur ${lines.length} lignes à insérer`, res.data.errors)
        console.log(res.data.errors.error)
        throw new Error('échec à l\'insertion des lignes dans le jeu de données')
      }
    }
  }

  if (processingConfig.clearFiles) {
    await fs.emptyDir(tmpDir)
  }
}
