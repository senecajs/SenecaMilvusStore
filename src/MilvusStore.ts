/* Copyright (c) 2024 Seneca contributors, MIT License */

import {
  MilvusClient,
  DataType,
  ConsistencyLevelEnum
} from '@zilliz/milvus2-sdk-node'

import { Gubu } from 'gubu'

const { Open, Any } = Gubu

type Options = {
  debug: boolean
  map?: any
  index: {
    prefix: string
    suffix: string
    map: Record<string, string>
    exact: string
  }
  field: {
    zone: { name: string }
    base: { name: string }
    name: { name: string }
    vector: { name: string }
  }
  cmd: {
    list: {
      size: number
    }
  }

  milvus: any
}

export type MilvusStoreOptions = Partial<Options>

function MilvusStore(this: any, options: Options) {
  const seneca: any = this

  const init = seneca.export('entity/init')

  let desc: any = 'MilvusStore'

  let client: any

  let store = {
    name: 'MilvusStore',

    save: function (this: any, msg: any, reply: any) {
      // const seneca = this
      const ent = msg.ent

      const canon = ent.canon$({ object: true })
      const collection = getCollection(ent, options)

      const body = ent.data$(false)

      // console.log('IN SAVE: ', collection, body)

      const fieldOpts: any = options.field

      async function doSave() {
        await createAndLoadCollection(client, { 
          schema: options.milvus.schema,
          collection: options.milvus.collection,
          collection_name: collection.name } )

          client.insert({
            collection_name: collection.name,
            data: [ body ],
          })
          .then( (res: any) => {
            let id = res.IDs.int_id.data[0]
            body.id = id
            reply(null, ent.data$(body))
          })
          .catch( (err: any) => {
            reply(err, null)
          })

      }

      doSave()



        /*
           ;['zone', 'base', 'name'].forEach((n: string) => {
           if ('' != fieldOpts[n].name && null != canon[n] && '' != canon[n]) {
           body[fieldOpts[n].name] = canon[n]
           }
           })

           const req = {
           index,
           body,
           }

           client
           .index(req)
           .then((res: any) => {
           const body = res.body
           ent.data$(body._source)
           ent.id = body._id
           reply(ent)
           })
           .catch((err: any) => reply(err))
         */
    },

    load: function (this: any, msg: any, reply: any) {
      // const seneca = this
      const ent = msg.ent
      console.log("LOADDD")

      // const canon = ent.canon$({ object: true })
      const index = resolveIndex(ent, options)

      let q = msg.q || {}

      reply({})

        /*
           if (null != q.id) {
           client
           .get({
           index,
           id: q.id,
           })
           .then((res: any) => {
           const body = res.body
           ent.data$(body._source)
           ent.id = body._id
           reply(ent)
           })
           .catch((err: any) => {
        // Not found
        if (err.meta && 404 === err.meta.statusCode) {
        reply(null)
        }

        reply(err)
        })


        } else {
        reply()
        }
         */
    },

    list: function (msg: any, reply: any) {
      // const seneca = this
      let vector
      const q = msg.q || {}
      const ent = msg.ent

      const index = resolveIndex(ent, options)
      const query = buildQuery({ index, options, msg })


      const collection = getCollection(ent, options)

      vector = q.vector

      query.vector = vector
      // console.log('LISTQ')
      // console.dir(query, { depth: null })

      if (null == query) {
        return reply([])
      }

      console.log('LIST QUERY: ', query)

      async function doList() {
        let res: any = await client.search(query)
        if(0 != res.status.code) {
          throw new Error(JSON.stringify(res.status))
        }
        let list = res.results.map((item: any) => ent.data$(item))
        reply(null, list)
      }

      doList()

        /*
           client
           .search(query)
           .then((res: any) => {
           const hits = res.body.hits
           const list = hits.hits.map((entry: any) => {
           let item = ent.make$().data$(entry._source)
           item.id = entry._id
           item.custom$ = { score: entry._score }
           return item
           })
           reply(list)
           })
           .catch((err: any) => {
           reply(err)
           })
         */
    },

    // NOTE: all$:true is REQUIRED for deleteByQuery
    remove: function (this: any, msg: any, reply: any) {
      // const seneca = this
      const ent = msg.ent

      const index = resolveIndex(ent, options)

      const q = msg.q || {}
      let id = q.id
      let query

      if (null == id) {
        query = buildQuery({ index, options, msg })

        if (null == query || true !== q.all$) {
          return reply(null)
        }
      }

      // console.log('REMOVE', id)
      // console.dir(query, { depth: null })

      reply(null)

      /*
         if (null != id) {
         client
         .delete({
         index,
         id,
        // refresh: true,
        })
        .then((_res: any) => {
        reply(null)
        })
        .catch((err: any) => {
        // Not found
        if (err.meta && 404 === err.meta.statusCode) {
        return reply(null)
        }

        reply(err)
        })
        } else if (null != query && true === q.all$) {
        client
        .deleteByQuery({
        index,
        body: {
        query,
        },
        // refresh: true,
        })
        .then((_res: any) => {
        reply(null)
        })
        .catch((err: any) => {
        // console.log('REM ERR', err)
        reply(err)
        })
        } else {
        reply(null)
        }
       */
    },

    close: function (this: any, _msg: any, reply: any) {
      this.log.debug('close', desc)
      reply()
    },

    // TODO: obsolete - remove from seneca entity
    native: function (this: any, _msg: any, reply: any) {
      reply(null, {
        client: () => client,
      })
    },
  }

  let meta = init(seneca, options, store)

  desc = meta.desc

  seneca.prepare(async function (this: any) {
    const address = options.milvus.address
    const token = options.milvus.token

    client = new MilvusClient({ address, token })

    console.log(client.createIndex, options.milvus.index)
    let out = await client.createIndex({
      collection_name: 'foo_chunk',
      field_name: 'vector',
      ...options.milvus.index,
    })
    console.log(out)

  })

  return {
    name: store.name,
    tag: meta.tag,
    exportmap: {
      native: () => {
        return { client }
      },
    },
  }
}

function buildQuery(spec: { index: string; options: any; msg: any }) {
  const { index, options, msg } = spec

  const q = msg.q || {}

  const fields = q.fields$ || []

  const collection = getCollection(msg.ent, options)

  let query: any = {
    collection_name: collection.name,
  }

  let index_config = options.milvus.index

  let outputFields: any = [ ...options.milvus.schema.map((field: any) => field.name), ...fields ]

  const parts = []
  /*
     for (let k in q) {
     if (!excludeKeys[k] && !k.match(/\$/)) {
     parts.push({
     match: { [k]: q[k] },
     })
     }
     }
   */

  const vector$ = msg.vector$ || q.directive$?.vector$
  if (vector$) {
    query['topk'] = null == vector$.k ? 11 : vector$.k
  }

  query = { 
    ...query,
    ...index_config['searchSettings'],
    output_fields: outputFields
  }

  return query
}

function resolveIndex(ent: any, options: Options) {
  let indexOpts = options.index
  if ('' != indexOpts.exact && null != indexOpts.exact) {
    return indexOpts.exact
  }

  let canonstr = ent.canon$({ string: true })
  indexOpts.map = indexOpts.map || {}
  if ('' != indexOpts.map[canonstr] && null != indexOpts.map[canonstr]) {
    return indexOpts.map[canonstr]
  }

  let prefix = indexOpts.prefix
  let suffix = indexOpts.suffix

  prefix = '' == prefix || null == prefix ? '' : prefix + '_'
  suffix = '' == suffix || null == suffix ? '' : '_' + suffix

  // TOOD: need ent.canon$({ external: true }) : foo/bar -> foo_bar
  let infix = ent
  .canon$({ string: true })
  .replace(/-\//g, '')
  .replace(/\//g, '_')

  return prefix + infix + suffix
}

function getCollection(ent: any, options: any) {
  let collection: any = {}

  let canon = ent.canon$({ object: true })

  collection.name = (null != canon.base ? canon.base + '_' : '') + canon.name

  return collection
}

async function createAndLoadCollection(client: any, config: any) {

  const collection: any = config.collection || {}
  const collection_name: any = config.collection_name || ''
  const schema: any = config.schema || []

  let coll: any

  coll = await client.createCollection({
    collection_name,
    fields: schema,
    ...collection,
  })

  coll = await client.loadCollection({
    collection_name,
    ...collection,
  })

}

// Default options.
const defaults: Options = {
  debug: false,
  map: Any(),
  index: {
    prefix: '',
    suffix: '',
    map: {},
    exact: '',
  },

  // '' === name => do not inject
  field: {
    zone: { name: 'zone' },
    base: { name: 'base' },
    name: { name: 'name' },
    vector: { name: 'vector' },
  },

  cmd: {
    list: {
      size: 8,
    },
  },

  milvus: Open({
    address: 'HOST:PORT',
    token: 'TOKEN',

    schema: [
      {
        name: 'id',
        description: 'ID field',
        data_type: DataType.Int64,
        is_primary_key: true,
        autoID: true,
      },
      {
        name: 'vector',
        description: 'Vector field',
        data_type: DataType.FloatVector,
        dim: 8,
      },
    ],
    collection: {
      consistency_level: ConsistencyLevelEnum.Strong,
      enable_dynamic_field: true,

    },
    index: {
      index_type: 'HNSW',
      params: { efConstruction: 512, M: 16 },
      metric_type: 'COSINE',
      searchSettings: {
        params: { ef: 512 },
        consistency_level: ConsistencyLevelEnum.Strong,
        metric_type: 'COSINE',    
      }
    }
  }),
}

Object.assign(MilvusStore, {
  defaults,
  utils: { resolveIndex },
})

export default MilvusStore

if ('undefined' !== typeof module) {
  module.exports = MilvusStore
}
