import fs from 'fs-extra'
import path from 'path'
import { createHash } from 'crypto'
import { parseStringPromise } from 'xml2js'
import dayjs from 'dayjs'
import customParseFormat from 'dayjs/plugin/customParseFormat.js'
import 'dayjs/locale/fr.js'
import type { AxiosInstance } from 'axios'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import { XML_FILE } from './download.ts'

dayjs.extend(customParseFormat)

type Log = ProcessingContext<ProcessingConfig>['log']

export type Line = {
  id: string
  latitude: number
  longitude: number
  cp: string
  code_DEP: string
  type_de_route: string
  adresse: string
  ville: string
  automate: boolean
  horaire: string
  services: string
  type_carburant: string
  prix_carburant: number
  maj_carburant?: string
  _id?: string
  _action?: string
  _score?: number
  status?: string
}
export type BulkLine = Partial<Line>

// qs requests are split so that the URL stays under this length
export const URL_LIMIT = 2500

const dayNames: Record<string, string> = { Lundi: 'Mo', Mardi: 'Tu', Mercredi: 'We', Jeudi: 'Th', Vendredi: 'Fr', Samedi: 'Sa', Dimanche: 'Su' }

/**
 * Turn the "instantane" XML file into one line per station and per fuel type.
 */
export const parseStations = async (xmlString: string): Promise<Line[]> => {
  const result = await parseStringPromise(xmlString, { attrkey: 'ATTR' })
  const tab: Line[] = []
  for (const station of result.pdv_liste.pdv as any[]) {
    if (station.prix === undefined) continue
    for (const carburant of station.prix) {
      // Base = line for the future csv file
      const base: Line = {
        id: station.ATTR.id,
        latitude: parseFloat((parseFloat(station.ATTR.latitude) / 100000).toFixed(6)),
        longitude: parseFloat((parseFloat(station.ATTR.longitude) / 100000).toFixed(6)),
        cp: station.ATTR.cp, // CP = postcode
        code_DEP: station.ATTR.cp.slice(0, 2),
        type_de_route: station.ATTR.pop,
        adresse: station.adresse[0].replace(/"/g, ''),
        ville: station.ville[0].replace(/"/g, '').toUpperCase().replace(/[0-9]+/g, '').trim(),
        automate: false,
        horaire: '',
        services: '',
        type_carburant: '',
        prix_carburant: 0
      }

      // Recovery of timetable of station
      if (station.horaires !== undefined) {
        base.automate = station.horaires[0].ATTR['automate-24-24'] !== ''
        let infoJour: string[] = []
        for (const jour of station.horaires[0].jour) {
          // Format the opening hours to https://schema.org/openingHours
          if (jour.ATTR.ferme !== '1' || base.automate) {
            let nomJour = dayNames[jour.ATTR.nom] ?? ''
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
        infoJour = infoJour.filter(elem => elem !== '')
        base.horaire = infoJour.join(',')
      }

      // Add the list of services available in the station
      if (station.services[0].service !== undefined) {
        station.services[0].service = station.services[0].service.map((elem: string) => elem.replace(/,/g, ' -'))
        base.services = station.services[0].service.join(',')
      } else base.services = station.services[0].trim()

      base.type_carburant = carburant.ATTR.nom.trim()
      base.prix_carburant = parseFloat(carburant.ATTR.valeur)
      // Convert the date to ISO 8601 format
      base.maj_carburant = dayjs(carburant.ATTR.maj, 'YYYY-MM-DD HH:mm:ss', 'fr').format()
      tab.push(base)
    }
  }
  return tab
}

// compare the published columns only, maj_carburant excluded: a new date with the same price is not a change
const hashLine = (line: BulkLine, keys: string[]) => createHash('md5').update(JSON.stringify(line, keys)).digest('hex')

/**
 * Build the lines to send to the _bulk_lines endpoint, or undefined when there is nothing to do.
 */
export default async (processingConfig: ProcessingConfig, tmpDir: string, axios: AxiosInstance, log: Log): Promise<BulkLine[] | undefined> => {
  await log.step('Traitement du fichier')
  // the source file is latin1 encoded
  const tab = await parseStations((await fs.readFile(path.join(tmpDir, XML_FILE))).toString('latin1'))

  const stats = { ajout: 0, modif: 0, modifSansMaj: 0, suppr: 0 }

  if (processingConfig.datasetMode === 'create') {
    stats.ajout = tab.length
    await log.info(`Création du jeu de donnée, ajout de ${stats.ajout} lignes`)
    return tab
  }

  const datasetUrl = `api/v1/datasets/${(processingConfig.dataset as { id: string }).id}`
  // dataUpdatedAt moves as soon as one line is written, even by a run that fails right after
  const lastUpdate = processingConfig.lastSync || (await axios.get(datasetUrl)).data.dataUpdatedAt
  if (!lastUpdate) {
    await log.error('Impossible de déterminer la date de dernière mise à jour des données')
    return
  }
  await log.info(`Dernière mise à jour des données: ${dayjs(lastUpdate).format('DD/MM/YYYY HH:mm:ss')}`)
  // tabFilter is the array containing fuel station that were updated after the last update
  let tabFilter: BulkLine[] = tab.filter((elem) => dayjs(elem.maj_carburant).isAfter(dayjs(lastUpdate)))
  let tabId = [...new Set(tabFilter.map(elem => elem.id))]

  await log.info(`Depuis la dernière mise à jour, il y a eu ${tabFilter.length} modifications sur ${tabId.length} stations uniques dans le fichier`)

  if (tabFilter.length <= 1) {
    await log.info('Rien à faire')
    return
  }

  // split the stringRequest because qs only accept regex of 1000 characters max
  let stringRequest = ''
  const ecart = 110
  do {
    stringRequest += `/${tabId.slice(0, ecart).join('|')}/`
    tabId = tabId.slice(ecart, tabId.length + 1)
  } while (tabId.length > ecart)
  if (tabId.length > 0) stringRequest += `/${tabId.slice(0, ecart).join('|')}/`

  const params: { size: number, qs?: string } = { size: 10000 }
  // data is the array containing all of the results of requests
  let data: Line[] = []
  const fetchLines = async () => {
    try {
      data = data.concat((await axios.get(datasetUrl + '/lines', { params })).data.results)
    } catch (err) {
      await log.info('Paramètres de requête ' + JSON.stringify(params))
      throw err
    }
  }

  // To avoid URL overflow, break at n char
  await log.info(`Limite URL : ${URL_LIMIT}`)
  await log.info(`Besoin de ${(stringRequest.length / URL_LIMIT + 1).toFixed(0)} requête(s) pour couvrir l'ensemble des modifications.`)
  let cpt = 0
  while (stringRequest.length > URL_LIMIT) {
    cpt++
    // get the closest line delimiter to slice the string well
    const firstIndex = stringRequest.indexOf('/') < stringRequest.indexOf('|') ? stringRequest.indexOf('/') : stringRequest.indexOf('|')
    const tmpString = stringRequest.slice(firstIndex + 1, URL_LIMIT)
    // depends of the last delimiter the end of input is not the same
    const lastGroup = tmpString.lastIndexOf('/')
    const lastNumber = tmpString.lastIndexOf('|')

    if (lastGroup > lastNumber) params.qs = `id:(/${tmpString.substring(0, lastGroup)})`
    else params.qs = `id:(/${tmpString.substring(0, lastNumber)}/)`

    await log.info(`Requête numéro ${cpt}`)
    await fetchLines()
    // get the next string
    stringRequest = stringRequest.substring(firstIndex + 1 + (lastGroup > lastNumber ? lastGroup : lastNumber), stringRequest.length)
  }
  if (stringRequest.length > 0) {
    // process the last group
    cpt++
    if (stringRequest.indexOf('|') < stringRequest.indexOf('/')) stringRequest = stringRequest.replace('|', '/')
    params.qs = `id:(${stringRequest})`
    await log.info(`Requête numéro ${cpt}`)
    await fetchLines()
  }

  await log.info('Début du filtre pour déterminer la nature des modifications')
  // find elements that are in the downloaded file but not in the current dataset
  for (const line of tabFilter) {
    if (!data.some(i => line.id === i.id && line.type_carburant === i.type_carburant)) {
      // createOrUpdate rather than create: the presence test above is made against the search
      // index, which can lag behind the stored lines. A 'create' on a line that does exist is
      // rejected (409), an upsert converges to the same state either way.
      line._action = 'createOrUpdate'
      stats.ajout++
    }
  }

  // find elements that must be updated (update on the line)
  // currCarbu is the current dataset value
  for (const currCarbu of data) {
    // requests gives us string with comma separated by space
    currCarbu.services = currCarbu.services?.split(',').map((elem) => elem.trim()).join(',')
    currCarbu.horaire = currCarbu.horaire?.split(',').map((elem) => elem.trim()).join(',')
    // find the correct line that may have change
    const line = tabFilter.find((elem) => elem.id === currCarbu.id && elem.type_carburant === currCarbu.type_carburant)
    if (line === undefined) continue
    const keys = Object.keys(line).filter(key => !key.startsWith('_') && key !== 'maj_carburant').sort()
    if (hashLine(line, keys) !== hashLine(currCarbu, keys)) {
      // update only when the price is different
      // see the comment on toAdd above: 'update' would be rejected (404) if the line is
      // in the search index but not in the stored lines
      line._action = 'createOrUpdate'
      line._id = currCarbu._id
      stats.modif++
    } else {
      line.status = 'delete'
      stats.modifSansMaj++
    }
  }

  // find elements that are in the dataset but no longer in the downloaded file
  const dataSupp: BulkLine[] = (await axios.get(datasetUrl + '/lines', { params: { sort: '_updatedAt', size: 4000, select: 'id,type_carburant,_id' } })).data.results

  for (const supp of dataSupp) {
    if (tab.some(i => i.id === supp.id && i.type_carburant === supp.type_carburant)) continue
    supp._action = 'delete'
    delete supp._score
    tabFilter.push(supp)
    stats.suppr++
  }

  tabFilter = tabFilter.filter((elem) => elem.status !== 'delete')
  await log.info(`Ajouts: ${stats.ajout}, Modifications: ${stats.modif}, Modifications sans changement: ${stats.modifSansMaj} Suppressions: ${stats.suppr}`)

  return tabFilter
}
