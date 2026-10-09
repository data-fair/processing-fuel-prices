import { describe, it, beforeEach } from 'node:test'
import assert from 'assert'
import fs from 'fs'
import os from 'os'
import path from 'path'
import type { AxiosInstance } from 'axios'
import { run, sendBatch, retryDelay } from '../lib/execute.ts'
import { SOURCE_URL } from '../lib/download.ts'

const zip = path.join(import.meta.dirname, 'resources', 'instantane.zip')
const log = { step: async () => {}, task: async () => {}, progress: async () => {}, info: async () => {}, error: async () => {}, warning: async () => {}, debug: async () => {} }

retryDelay.ms = 0

type BulkResponse = { status?: number, data?: { nbErrors: number, errors: { line: number, status: number }[] } }

const fakeAxios = (bulkResponses: BulkResponse[]) => {
  const posts: { url: string, body: any }[] = []
  const axios = {
    post: async (url: string, body: any) => {
      posts.push({ url, body })
      if (url === 'api/v1/datasets') return { data: { id: 'ds', title: body.title } }
      const res = bulkResponses.shift() ?? {}
      if (res.status) throw Object.assign(new Error('http error'), { status: res.status })
      return { data: res.data ?? { nbErrors: 0 } }
    },
    get: async (url: string) => {
      if (url === SOURCE_URL) return { data: fs.createReadStream(zip) }
      if (url === 'api/v1/datasets/ds') return { data: { id: 'ds', title: 'Prix', dataUpdatedAt: '2026-10-01T00:00:00Z' } }
      return { data: { results: [] } }
    }
  } as unknown as AxiosInstance
  return { axios, posts }
}

describe('sendBatch', () => {
  const lines = [{ id: '1', _action: 'createOrUpdate' }, { id: '2', _id: 'b', _action: 'delete' }]

  it('tolerates the deletion of a line already gone', async () => {
    const { axios, posts } = fakeAxios([{ data: { nbErrors: 1, errors: [{ line: 1, status: 404 }] } }])
    assert.equal(await sendBatch(axios, log, 'ds', lines), 1)
    assert.equal(posts.length, 1)
  })

  it('sends the batch again after transient line errors (elasticsearch overload)', async () => {
    const { axios, posts } = fakeAxios([{ data: { nbErrors: 1, errors: [{ line: 0, status: 429 }] } }, {}])
    assert.equal(await sendBatch(axios, log, 'ds', lines), 0)
    assert.equal(posts.length, 2)
  })

  it('sends the batch again after a transient http error', async () => {
    const { axios, posts } = fakeAxios([{ status: 503 }, {}])
    assert.equal(await sendBatch(axios, log, 'ds', lines), 0)
    assert.equal(posts.length, 2)
  })

  it('gives up after 3 attempts', async () => {
    const { axios, posts } = fakeAxios([{ status: 503 }, { status: 503 }, { status: 503 }, {}])
    await assert.rejects(sendBatch(axios, log, 'ds', lines))
    assert.equal(posts.length, 3)
  })

  it('fails right away on a real line error', async () => {
    const { axios, posts } = fakeAxios([{ data: { nbErrors: 1, errors: [{ line: 0, status: 400 }] } }])
    await assert.rejects(sendBatch(axios, log, 'ds', lines), /échec à l'insertion/)
    assert.equal(posts.length, 1)
  })

  it('never ignores errors that are not detailed', async () => {
    const undetailed = { data: { nbErrors: 60, errors: [{ line: 1, status: 404 }] } }
    const { axios, posts } = fakeAxios([undetailed, undetailed, undetailed])
    await assert.rejects(sendBatch(axios, log, 'ds', lines))
    assert.equal(posts.length, 3)
  })
})

describe('run', () => {
  let tmpDir: string
  let processingConfig: Record<string, unknown>
  let patches: Record<string, unknown>[]
  // the worker applies a patch to the config in place
  const patchConfig = async (patch: Record<string, unknown>) => {
    patches.push(patch)
    Object.assign(processingConfig, patch)
  }
  const context = (axios: AxiosInstance) => ({ processingConfig, tmpDir, axios, log, patchConfig, processingId: 'p1' }) as any

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'fuel-prices-'))
    patches = []
  })

  it('creates the dataset, switches to update and saves the newest source date', async () => {
    processingConfig = { datasetMode: 'create', datasetTitle: 'Prix test' }
    const { axios, posts } = fakeAxios([])
    await run(context(axios))
    assert.equal(posts[0].body.title, 'Prix test')
    assert.deepEqual(posts[0].body.extras, { processingId: 'p1' })
    assert.deepEqual(patches[0], { datasetMode: 'update', dataset: { id: 'ds', title: 'Prix test' }, lastSync: new Date(0).toISOString() })
    assert.equal(posts[1].url, 'api/v1/datasets/ds/_bulk_lines')
    assert.equal(posts[1].body.length, 3, 'every line of the file is sent on creation')
    assert.equal(patches.length, 2)
    assert.equal(patches[1].lastSync, new Date(posts[1].body.find((l: any) => l.type_carburant === 'E10').maj_carburant).toISOString())
  })

  it('keeps lastSync when a batch fails, so the next run sends the lines again', async () => {
    processingConfig = { datasetMode: 'update', dataset: { id: 'ds', title: 'Prix' } }
    const { axios } = fakeAxios([{ data: { nbErrors: 1, errors: [{ line: 0, status: 400 }] } }])
    await assert.rejects(run(context(axios)))
    assert.deepEqual(patches, [])
  })
})
