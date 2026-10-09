import { describe, it } from 'node:test'
import assert from 'assert'
import processingConfigSchema from '../processing-config-schema.json' with { type: 'json' }

describe('Fuel prices processing', () => {
  it('exposes a processing config schema for users', () => {
    assert.equal(processingConfigSchema.type, 'object')
  })

  it('carries no legacy vjsf 2 keyword', () => {
    const legacy = JSON.stringify(processingConfigSchema).match(/"x-(?!i18n-|exports)[\w-]+"/g)
    assert.equal(legacy, null)
  })
})
