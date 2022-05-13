const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const exec = util.promisify(require('child_process').exec)
const pump = util.promisify(require('pump'))

module.exports = async (pluginConfig, dir = 'data', axios, log) => {
  await log.step('Téléchargement du fichier instantané')
  const fileName = path.parse(new URL(pluginConfig.url).pathname).name + '.zip'
  const file = `${dir}/${fileName}`

  console.log(`download ${pluginConfig.url} -> ${file}`)
  // this is used only in dev
  if (await fs.pathExists(file)) {
    await log.info(`Le fichier ${file} existe déjà`)
  } else {
    // creating empty file before streaming seems to fix some weird bugs with NFS
    await fs.ensureFile(file)
    await log.info('Télécharge le fichier ' + pluginConfig.url)
    try {
      const res = await axios.get(pluginConfig.url, { responseType: 'stream' })
      await pump(res.data, fs.createWriteStream(file))
    } catch (err) {
      if (err.status === 404) {
        await fs.remove(file)
        return
      }
      throw err
    }

    // Try to prevent weird bug with NFS by forcing syncing file before reading it
    const fd = await fs.open(file, 'r')
    await fs.fsync(fd)
    await fs.close(fd)
  }

  console.log(`unzip ${file}`)
  await exec(`unzip -o ${file}`)
  const fileXML = (await fs.readdir('.')).filter(file => file.endsWith('.xml') && file.toUpperCase().includes('CARBURANT'))
  await fs.rename(fileXML[0], 'carburants.xml')
  // remove the zip file
  await fs.remove(fileName)
}
