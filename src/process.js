const xml2js = require('xml2js')
const parser = new xml2js.Parser({ attrkey: 'ATTR' })
const fs = require('fs')
const path = require('path')
const iconv = require('iconv-lite')
const dayjs = require('dayjs')
const customParseFormat = require('dayjs/plugin/customParseFormat')
dayjs.extend(customParseFormat)

require('dayjs/locale/fr')
const md5 = require('md5')

module.exports = async (pluginConfig, processingConfig, tmpDir, axios, log) => {
  await log.step('Traitement du fichier')
  const tab = []
  // Change the encoding to UTF-8
  const xmlString = iconv.decode(fs.readFileSync(path.join(tmpDir, 'carburants.xml')), 'iso-8859-1')
  // Put in string all the xml file contents
  parser.parseString(xmlString, function (error, result) {
    if (error === null) {
      const donnee = result.pdv_liste.pdv
      for (const station of donnee) {
        if (station.prix !== undefined) {
          for (const carburant of station.prix) {
            // Base = line for the future csv file
            const base = {
              id: station.ATTR.id,
              latitude: parseFloat((parseFloat(station.ATTR.latitude) / 100000).toFixed(6)),
              longitude: parseFloat((parseFloat(station.ATTR.longitude) / 100000).toFixed(6)),
              cp: station.ATTR.cp, // CP = postcode
              code_DEP: station.ATTR.cp.substr(0, 2),
              type_de_route: station.ATTR.pop,

              adresse: station.adresse[0].replace(/"/g, ''),
              ville: station.ville[0].replace(/"/g, '').toUpperCase().replace(/[0-9]+/g, '').trim(),
              automate: false
            }

            // Recovery of timetable of station
            if (station.horaires !== undefined) {
              base.automate = station.horaires[0].ATTR['automate-24-24'] !== ''
              let infoJour = []
              for (const jour of station.horaires[0].jour) {
                // Format the opening hours to https://schema.org/openingHours
                if (jour.ATTR.ferme !== '1' || base.automate) {
                  let nomJour = ''
                  switch (jour.ATTR.nom) {
                    case 'Lundi':
                      nomJour = 'Mo'
                      break
                    case 'Mardi':
                      nomJour = 'Tu'
                      break
                    case 'Mercredi':
                      nomJour = 'We'
                      break
                    case 'Jeudi':
                      nomJour = 'Th'
                      break
                    case 'Vendredi':
                      nomJour = 'Fr'
                      break
                    case 'Samedi':
                      nomJour = 'Sa'
                      break
                    case 'Dimanche':
                      nomJour = 'Su'
                      break
                  }

                  if (jour.horaire !== undefined) {
                    const open = jour.horaire[0].ATTR.ouverture.replace(/\./, ':')
                    const close = jour.horaire[0].ATTR.fermeture.replace(/\./, ':')
                    const hour = open + '-' + close
                    let ouvertures
                    if (infoJour.length > 0) {
                      const arrayJour = infoJour[infoJour.length - 1].split(' ')
                      if (infoJour[infoJour.length - 1] === '') infoJour[infoJour.length - 1] = nomJour
                      if ((open === '00:00' && (close.split(':')[0] === '23' && parseInt(close.split(':')[1]) > 50)) || (open === close)) {
                        if (arrayJour[1] === hour || arrayJour.length <= 1) {
                          nomJour = arrayJour[0].split('-')[0] + '-' + nomJour
                          infoJour.splice(infoJour.length - 1)
                          ouvertures = nomJour
                        } else {
                          ouvertures = nomJour
                        }
                      } else if (arrayJour[1] === hour) {
                        nomJour = arrayJour[0].split('-')[0] + '-' + nomJour
                        infoJour.splice(infoJour.length - 1)
                        ouvertures = nomJour + ' ' + hour
                      } else {
                        ouvertures = nomJour + ' ' + hour
                      }
                    } else {
                      ouvertures = nomJour + ' ' + hour
                    }
                    infoJour.push(ouvertures)
                  }
                } else infoJour.push('')
              }
              infoJour = infoJour.filter(elem => ![''].includes(elem))
              base.horaire = infoJour.join(',')
            } else {
              base.horaire = ''
            }

            // Add the list of services available in the station
            if (station.services[0].service !== undefined) {
              station.services[0].service = station.services[0].service.map(elem => elem.replace(/,/g, ' -'))
              base.services = station.services[0].service.join(',')
            } else if (station.services[0].trim().length === 0) base.services = ''
            else base.services = station.services[0].trim()

            base.type_carburant = carburant.ATTR.nom.trim()
            base.prix_carburant = parseFloat(carburant.ATTR.valeur)
            // Convert the date to ISO 8601 format
            base.maj_carburant = dayjs(carburant.ATTR.maj, 'YYYY-MM-DD HH:mm:ss', 'fr').format()
            tab.push(base)
          }
        }
      }
    } else {
      console.log(error)
      throw error
    }
  })

  const stats = {
    ajout: 0,
    modif: 0,
    modifSansMaj: 0,
    suppr: 0
  }

  if (processingConfig.datasetMode === 'create') {
    stats.ajout = tab.length
    await log.info(`Création du jeu de donnée, ajout de ${stats.ajout} lignes`)
    return tab
  } else if (processingConfig.datasetMode === 'update') {
    const lastUpdate = (await axios.get(processingConfig.dataset.href)).data.dataUpdatedAt
    if (lastUpdate) {
      await log.info(`Dernière mise à jour des données: ${dayjs(lastUpdate).format('DD/MM/YYYY HH:mm:ss')}`)
      // tabFilter is the array containing fuel station that were updated after the last update
      let tabFilter = tab.filter((elem) => dayjs(elem.maj_carburant).isAfter(dayjs(lastUpdate)))
      let tabId = [...new Set(tabFilter.map(elem => elem.id))]

      await log.info(`Depuis la dernière mise à jour, il y a eu ${tabFilter.length} modifications sur ${tabId.length} stations uniques dans le fichier`)

      // split the stringRequest because qs only accept regex of 1000 characters max
      let stringRequest = ''
      const ecart = 110
      do {
        stringRequest += `/${tabId.slice(0, ecart).join('|')}/`
        tabId = tabId.slice(ecart, tabId.length + 1)
      } while (tabId.length > ecart)
      if (tabId.length > 0) stringRequest += `/${tabId.slice(0, ecart).join('|')}/`

      const params = {
        size: 10000
      }

      if (tabFilter.length > 1) {
        // data is the array containing all of the results of requests
        let data = []

        // To avoid URL overflow, break at n char
        await log.info(`Limite URL : ${pluginConfig.limit}`)
        const breakRequestAt = pluginConfig.limit
        await log.info(`Besoin de ${(stringRequest.length / breakRequestAt + 1).toFixed(0)} requête(s) pour couvrir l'ensemble des modifications.`)
        let cpt = 0
        while (stringRequest.length > breakRequestAt) {
          cpt++
          // get the closest line delimiter to slice the string well
          const firstIndex = stringRequest.indexOf('/') < stringRequest.indexOf('|') ? stringRequest.indexOf('/') : stringRequest.indexOf('|')
          const tmpString = stringRequest.slice(firstIndex + 1, breakRequestAt)
          // depends of the last delimiter the end of input is not the same
          const lastGroup = tmpString.lastIndexOf('/')
          const lastNumber = tmpString.lastIndexOf('|')

          if (lastGroup > lastNumber) params.qs = `id:(/${tmpString.substring(0, lastGroup)})`
          else params.qs = `id:(/${tmpString.substring(0, lastNumber)}/)`

          await log.info(`Requête numéro ${cpt}`)
          // process the requests and add the result to the data array
          try {
            data = data.concat((await axios.get(processingConfig.dataset.href + '/lines', { params })).data.results)
          } catch (err) {
            await log.error(err)
            await log.info('Paramètres de requête ' + JSON.stringify(params))
            throw err
          }
          // get the next string
          stringRequest = stringRequest.substring(firstIndex + 1 + (lastGroup > lastNumber ? lastGroup : lastNumber), stringRequest.length)
        }
        if (stringRequest.length > 0) {
          // process the last group
          cpt++
          if (stringRequest.indexOf('|') < stringRequest.indexOf('/')) stringRequest = stringRequest.replace('|', '/')
          params.qs = `id:(${stringRequest})`
          await log.info(`Requête numéro ${cpt}`)
          try {
            data = data.concat((await axios.get(processingConfig.dataset.href + '/lines', { params })).data.results)
          } catch (err) {
            await log.error(err)
            await log.info('Paramètres de requête ' + JSON.stringify(params))
            throw err
          }
        }

        await log.info('Début du filtre pour déterminer la nature des modifications')
        // find elements that are in the downloaded file but not in the current dataset
        const toAdd = tabFilter.filter(o => !data.some(i => o.id === i.id && o.type_carburant === i.type_carburant))

        for (const line of toAdd) {
          tabFilter.find((elem) => elem.id === line.id && elem.type_carburant === line.type_carburant)._action = 'create'
          stats.ajout++
        }

        // find elements that must be updated (update on the line)
        // currCarbu is the current dataset value
        for (const currCarbu of data) {
          // requests gives us string with comma separated by space
          currCarbu.services = currCarbu.services?.split(',').map((elem) => elem.trim()).join(',')
          currCarbu.horaire = currCarbu.horaire?.split(',').map((elem) => elem.trim()).join(',')
          // find the correct line that may have change
          const line = tabFilter.find((elem) => elem.id === currCarbu.id && elem.type_carburant === currCarbu.type_carburant)
          if (line !== undefined) {
            const stoMaj = line.maj_carburant
            delete currCarbu.maj_carburant
            delete line.maj_carburant
            line._id = currCarbu._id
            const lineInTab = tabFilter.find((elem) => elem.id === line.id && elem.type_carburant === line.type_carburant)

            if (md5(JSON.stringify(line, Object.keys(line).filter(key => !key.startsWith('_')).sort())) !== md5(JSON.stringify(currCarbu, Object.keys(line).filter(key => !key.startsWith('_')).sort()))) {
              // update only when the price is different
              lineInTab._action = 'update'
              lineInTab._id = currCarbu._id
              stats.modif++
            } else {
              lineInTab.status = 'delete'
              stats.modifSansMaj++
            }
            line.maj_carburant = stoMaj
          }
        }

        // find elements that are in the dataset but no longer in the downloaded file
        const dataSupp = (await axios.get(processingConfig.dataset.href + '/lines', { params: { sort: '_updatedAt', size: 4000, select: 'id,type_carburant,_id' } })).data.results

        const toSupp = dataSupp.filter(o => !tab.some(i => (i.id === o.id && i.type_carburant === o.type_carburant)))
        for (const supp of toSupp) {
          supp._action = 'delete'
          delete supp._score
          tabFilter.push(supp)
          stats.suppr++
        }

        tabFilter = tabFilter.filter((elem) => elem.status !== 'delete')
        await log.info(`Ajouts: ${stats.ajout}, Modifications: ${stats.modif}, Modifications sans changement: ${stats.modifSansMaj} Suppressions: ${stats.suppr}`)

        return tabFilter
      } else {
        // tabFilter == 0 => nothing to do
        await log.info('Rien à faire')
      }
    } else {
      // lastUpdate is undef
      await log.error('Impossible de déterminer la date de dernière mise à jour des données')
    }
  }
}
