const xml2js = require('xml2js')
const parser = new xml2js.Parser({ attrkey: 'ATTR' })
const fs = require('fs')
const path = require('path')
const endOfLine = require('os').EOL
const datasetSchema = require('./schema.json')
const iconv = require('iconv-lite')

module.exports = async (tmpDir, log) => {
  await log.step('Traitement des fichiers')
  const outFile = await fs.promises.open(path.join(tmpDir, 'carburant.csv'), 'w')
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
              const infoJour = []
              for (const jour of station.horaires[0].jour) {
                // Format the opening hours to https://schema.org/openingHours
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
                let ouvertures = nomJour + ' '

                if (jour.horaire !== undefined) {
                  ouvertures += jour.horaire[0].ATTR.ouverture.replace(/\./, ':') + '-'
                  ouvertures += jour.horaire[0].ATTR.fermeture.replace(/\./, ':')
                  infoJour.push(ouvertures)
                }
                base.horaire = infoJour.join(';')
              }
            } else {
              base.horaire = ''
            }

            // Add the list of services available in the station
            if (station.services[0].service !== undefined) {
              base.services = '"' + station.services[0].service.join(';') + '"'
            } else {
              base.services = station.services[0]
            }

            base.type_carburant = carburant.ATTR.nom
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
