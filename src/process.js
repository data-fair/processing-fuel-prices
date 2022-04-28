const xml2js = require('xml2js')
const parser = new xml2js.Parser({ attrkey: 'ATTR' })
const fs = require('fs')
const path = require('path')
const endOfLine = require('os').EOL
const datasetSchema = require('./schema.json')
const iconv = require('iconv-lite')

module.exports = async (tmpDir, log) => {
  await log.step('Traitement du fichier')
  const outFile = await fs.promises.open(path.join(tmpDir, 'carburants.csv'), 'w')
  await outFile.write(datasetSchema.map(f => `"${f.key}"`).join(',') + endOfLine)

  // Change the encoding to UTF-8
  const xmlString = iconv.decode(fs.readFileSync('PrixCarburants_instantane.xml'), 'iso-8859-1')
  // Put in string all the xml file contents
  parser.parseString(xmlString, function (error, result) {
    if (error === null) {
      const donnee = result.pdv_liste.pdv

      for (const station of donnee) {
        if (station.prix !== undefined) {
          for (const carburant of station.prix) {
            // Recover the first data (easy to take from the database)

            // Base = line for the future csv file
            const base = {
              id: station.ATTR.id,
              latitude: (parseFloat(station.ATTR.latitude) / 100000).toFixed(6),
              longitude: (parseFloat(station.ATTR.longitude) / 100000).toFixed(6),
              cp: station.ATTR.cp, // CP = postcode
              code_DEP: station.ATTR.cp.substr(0, 2),
              type_de_route: station.ATTR.pop,

              adresse: '"' + station.adresse[0].replace(/"/g, '') + '"',
              ville: '"' + station.ville[0].replace(/"/g, '').toUpperCase().replace(/[0-9]+/g, '').trim() + '"',
              automate: '0'
            }

            // Recovery of timetable of station
            if (station.horaires !== undefined) {
              base.automate = station.horaires[0].ATTR['automate-24-24'] === '' ? '0' : '1'
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
              base.horaire = '"' + infoJour.join(',') + '"'
            } else {
              base.horaire = ''
            }

            // Add the list of services available in the station
            if (station.services[0].service !== undefined) base.services = '"' + station.services[0].service.join(',') + '"'
            else if (station.services[0].trim().length === 0) base.services = ''
            else base.services = station.services[0].trim()

            base.type_carburant = carburant.ATTR.nom.trim()
            base.prix_carburant = parseFloat(carburant.ATTR.valeur)
            // Convert the date to ISO 8601 format
            const date = new Date(carburant.ATTR.maj).toISOString()
            base.maj_carburant = '"' + date + '"'

            // Write the current line in the output file
            outFile.write(Object.values(base).join(',') + endOfLine)
          }
        }
      }
    } else {
      console.log(error)
    }
  })
  await outFile.close()
}
