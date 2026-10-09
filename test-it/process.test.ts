import { describe, it } from 'node:test'
import assert from 'assert'
import fs from 'fs'
import os from 'os'
import path from 'path'
import { promisify } from 'util'
import { execFile } from 'child_process'
import type { AxiosInstance } from 'axios'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import processData, { parseStations, type Line } from '../lib/process.ts'

const zip = path.join(import.meta.dirname, 'resources', 'instantane.zip')
const log = { step: async () => {}, task: async () => {}, progress: async () => {}, info: async () => {}, error: async () => {}, warning: async () => {}, debug: async () => {} }

const extractSample = async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'fuel-prices-'))
  await promisify(execFile)('unzip', ['-o', zip, '-d', dir])
  fs.renameSync(path.join(dir, 'PrixCarburants_instantane.xml'), path.join(dir, 'carburants.xml'))
  return dir
}

// maj_carburant depends on the timezone of the process, see the comparison without it
const withoutDate = ({ maj_carburant: maj, ...line }: Partial<Line>) => line

describe('parseStations', () => {
  it('builds one line per station and fuel type, as the previous version did', async () => {
    const dir = await extractSample()
    const tab = await parseStations(fs.readFileSync(path.join(dir, 'carburants.xml')).toString('latin1'))
    assert.equal(tab.length, 3, 'the station without prices is skipped')
    assert.deepEqual(withoutDate(tab[0]), {
      id: '75000001',
      latitude: 48.85,
      longitude: 2.35,
      cp: '75001',
      code_DEP: '75',
      type_de_route: 'A',
      adresse: '1 rue de Rivoli',
      ville: 'PARIS ER',
      automate: false,
      horaire: 'Mo-Fr 07:00-20:00,Sa 08:00-12:00',
      services: 'Lavage - aspirateur,Boutique alimentaire',
      type_carburant: 'Gazole',
      prix_carburant: 2.319
    })
    assert.match(tab[0].maj_carburant!, /^2026-10-07T08:54:39[+-]\d\d:\d\d$/)
    assert.equal(tab[1].ville, 'SAINT-ÉTIENNE', 'the latin1 source is decoded')
    assert.equal(tab[1].horaire, '')
    assert.equal(tab[1].services, '')
    assert.deepEqual(tab.map(l => l.type_carburant), ['Gazole', 'SP98', 'E10'])
  })
})

describe('processData in update mode', () => {
  it('upserts new and changed lines, skips unchanged ones and deletes lines gone from the source', async () => {
    const dir = await extractSample()
    const tab = await parseStations(fs.readFileSync(path.join(dir, 'carburants.xml')).toString('latin1'))
    // the API returns multi-valued strings with ", " separators
    const stored = [
      { ...tab[0], _id: 'a', services: tab[0].services.split(',').join(', '), maj_carburant: '2020-01-01T00:00:00+00:00' },
      { ...tab[1], _id: 'b', prix_carburant: 1.5 }
    ]
    const requests: string[] = []
    const axios = {
      get: async (url: string, opts?: { params?: Record<string, unknown> }) => {
        requests.push(url)
        if (opts?.params?.qs) return { data: { results: structuredClone(stored) } }
        if (opts?.params?.sort) {
          return { data: { results: [{ id: '99999999', type_carburant: 'Gazole', _id: 'x', _score: 1 }, { id: tab[0].id, type_carburant: 'Gazole', _id: 'a' }] } }
        }
        throw new Error('unexpected request ' + url)
      }
    } as unknown as AxiosInstance

    const bulk = await processData({ datasetMode: 'update', dataset: { id: 'ds' }, lastSync: '2026-10-01T00:00:00.000Z' } as ProcessingConfig, dir, axios, log)
    assert.ok(bulk)
    assert.ok(!requests.includes('api/v1/datasets/ds'), 'lastSync replaces dataUpdatedAt')
    const summary = bulk.map(l => [l.id, l.type_carburant, l._action, l._id])
    assert.deepEqual(summary, [
      ['42000001', 'SP98', 'createOrUpdate', 'b'],
      ['42000001', 'E10', 'createOrUpdate', undefined],
      ['99999999', 'Gazole', 'delete', 'x']
    ])
    assert.equal(bulk[2]._score, undefined)
  })

  it('falls back to dataUpdatedAt and only keeps lines updated after it', async () => {
    const dir = await extractSample()
    const axios = {
      get: async (url: string, opts?: { params?: Record<string, unknown> }) => {
        if (url === 'api/v1/datasets/ds') return { data: { dataUpdatedAt: '2026-10-07T12:00:00Z' } }
        return { data: { results: [] } }
      }
    } as unknown as AxiosInstance
    const bulk = await processData({ datasetMode: 'update', dataset: { id: 'ds' } } as ProcessingConfig, dir, axios, log)
    assert.deepEqual(bulk?.map(l => l.type_carburant), ['SP98', 'E10'])
  })
})
