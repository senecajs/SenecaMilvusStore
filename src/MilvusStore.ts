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
    map: Record<string, string>
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

// QUERY_TYPE
const QUERY_ENUM = 0
const SEARCH_ENUM = 1
const GET_ENUM = 2

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
      const q = msg.q || {}

      const collection_name = makeCollectionName(ent.canon$({ string: true }))

      const body = ent.data$(false)

      const id = q.id || ent.id

      // console.log('IN SAVE: ', collection, body)

      async function doSave() {
        await loadCollection(client, {
          collection_name,
          collection: options.milvus.collection,
        })

        if(null == id) {
          let res = await client.insert({
            collection_name,
            data: [ body ],
          })
          checkError(res, reply)

          let id = res.IDs.int_id.data[0]
          body.id = id
          reply(null, ent.make$(body))
        } else {
          // console.log("IN UPSERT", body)
          reply(new Error("UPSERT NOT SUPPORTED"), null)
          /*
             client.upsert({
             collection_name,
             fields_data: [ body ],
             })
             .then( (res: any) => {
             console.log(res)
             reply(null, ent.make$().data$(body))
             })
             .catch( (err: any) => {
             reply(err, null)
             })
           */

        }

      }

      doSave()

    },

    load: function (this: any, msg: any, reply: any) {
      // const seneca = this
      const ent = msg.ent
      
      let query = buildQuery({ seneca, options, msg }, GET_ENUM)

      let q = msg.q || {}

      // console.log('IN LOAD: ', query)

      async function doLoad() {
        await loadCollection(client, {
          collection_name: query.collection_name,
          collection: options.milvus.collection,
        })

        let res = await client.get(query)

        checkError(res, reply)

        if( null == res.data[0]) {
          return reply(null)
        }

        reply(null, ent.make$(res.data[0]))
      }

      doLoad()
    },

    list: function (msg: any, reply: any) {
      // const seneca = this
      
      
      const q = msg.q || {}
      const ent = msg.ent
      
      const vector = q.vector
      
      let query_type: Number
      
      if(vector) {
        query_type = SEARCH_ENUM
      } else {
        query_type = QUERY_ENUM
      }

      const query = buildQuery({ seneca, options, msg }, query_type)
      

      if (null == query) {
        return reply([])
      }
      
      // console.log('IN QUERY: ', query)

      async function doList() {
        // Load collection in memory
        await loadCollection(client, {
          collection_name: query.collection_name,
          collection: options.milvus.collection,
        })
        
        let res: any
        
        if(query_type == SEARCH_ENUM) {
          // console.log('LIST SEARCH: ', query )
          res = await client.search(query)
        } else if(query_type == QUERY_ENUM) {
          res = await client.query(query)
        }
        
        checkError(res, reply)
        
        reply(null, (res.results || res.data).map((item: any) => {
          let meta = item.$meta 
          if(meta) {
            for(let key in meta) {
              item[key] = meta[key]
            }
            delete item.$meta
          }
          return ent.make$(item)
        }))

      }

      doList()

    },

    // NOTE: all$:true is REQUIRED for deleteByQuery
    remove: function (this: any, msg: any, reply: any) {
      // const seneca = this
      const ent = msg.ent

      const q = msg.q || {}
      let id = q.id
      
      const collection_name = makeCollectionName(ent.canon$({ string: true }))
      
      // console.log('REMOVE: ', collection_name)
      
      if (null == id) {
      
        

        if (null == id || true !== q.all$) {
          return reply(null)
        }
      }

      // console.log('REMOVE', q)
      
      async function doRemove() {
      
        if( null != id) {
        
          let res = await client.delete({
            collection_name,
            ids: [ id ],
          })
          // console.log("DELETE: ", res, id)
          
          checkError(res, reply)
          
          return reply(null)
          
        } else if( true === q.all$) {
        /*
          let res = await client.deleteEntities({
            collection_name: query.collection_name,
            // expr: '', // needs a filter or expr
          })
          
          checkError(res, reply)
          
          console.log('res: ', res)
        */
        }

        reply(null)
      }
      
      doRemove()
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
    
    // console.log("IN PREPARE: ", address, token)



    // console.log('IN PREPARE: ', client.createIndex, options.milvus.index, options.map)

    for(let canon in options.map) {
      let res

      let collection_name = makeCollectionName(canon)
      
      let collection_exists = await client.hasCollection({ collection_name })

      checkError(collection_exists)
      
      if(collection_exists.value) {
        continue
      }

      res = await client.createCollection({
        collection_name,
        fields: options.milvus.schema,
        ...options.milvus.collection,
      })
      checkError(res)

      // console.log('IN COLL: ', res)

      // TODO: FEATURE TO INDEX OTHER FIELDS
      res = await client.createIndex({
        collection_name,
        field_name: 'vector',
        ...options.milvus.index,
      })
      checkError(res)

      // console.log('IN INDEX: ', res)
    }



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

function makeCollectionName(canon: String) {

/*
  let [ zone, base, name ] = canon.split('/')

  zone = '-' == zone ? '' : zone
  base = '-' == base ? '' : base
  name = '-' == name ? '' : name

  let str = [ zone, base, name ].filter((v: String)=>null!=v&&''!=v).join('_')
 */
 
  let str = canon
    .replace(/-\//g, '')
    .replace(/\//g, '_')
    

  return str
}

function buildQuery(spec: { seneca: any, options: any; msg: any }, QUERY_TYPE: Number = 0) {
  const { seneca, options, msg } = spec

  const ent = msg.ent
  const q = msg.q || {}

  const fields = q.fields$ || []

  const collection_name = makeCollectionName(ent.canon$({ string: true }))

  // let cq = seneca.util.clean(q)
  
  // no query params means no results
  if(0 === Object.keys(q).length) {
    return null
  }

  let query: any = {
    collection_name,
  }

  let index_config = options.milvus.index

  let outputFields: any = [ ...options.milvus.schema.map((field: any) => field.name), ...fields ]
  
  let vector
  let cq = seneca.util.clean(q)
      
  if(cq.vector) {
    vector = cq.vector
    delete cq.vector
  }
      
  let expr = Object.keys(cq).map(c => {
    return build_cmps(cq[c], c).cmps.map(cmp => {
      outputFields.push(cmp.k)
      return cmp.k + cmp.cmpop + JSON.stringify(cmp.v)
    }).join('and')
  }).join('or')

  if(QUERY_TYPE == SEARCH_ENUM) {
    const vector$ = msg.vector$ || q.directive$?.vector$
    if (vector$) {
      query['topk'] = null == vector$.k ? 11 : vector$.k
    }

    query = { 
      ...query,
      vector,
      filter: expr,
      ...index_config['searchSettings'],
      output_fields: outputFields
    }
  
  } else if(QUERY_TYPE == QUERY_ENUM) {
    query = {
      ...query,
      limit: 100 || q.limit$,
      expr,
      output_fields: outputFields,
    }
    
  } else if(QUERY_TYPE == GET_ENUM) {
    query = {
      ...query,
      ids: [ q.id ],
      output_fields: outputFields,
    }
  }

  return query
  
}

async function loadCollection(client: any, config: any) {
  const collection_name: any = config.collection_name || ''
  const collection: any = config.collection || {}


  let res: any

  res = await client.loadCollection({
    collection_name,
    ...collection,
  })

  checkError(res)

}

function build_cmps(qv: any, kname: String) {
  // console.log('QV: ', typeof qv, qv)

  if ('object' != typeof qv) {
    //  && !Array.isArray(qv)) {
    return { cmps: [{ c: 'eq$', cmpop: '==', k: kname, v: qv }] }
  }

  let cmpops: any = {
    gt$: { cmpop: '>' },
    gte$: { cmpop: '>=' },
    lt$: { cmpop: '<' },
    lte$: { cmpop: '<=' },
    ne$: { cmpop: '!=' },
    eq$: { cmpop: '==' },
  },
  cmps = []

  for (let k in qv) {
    let cmp = cmpops[k]
    if (cmp) {
      cmp = { ...cmpops[k] }
      cmp.k = kname
      cmp.v = qv[k]
      cmp.c = k
      cmps.push(cmp)
    } else if (k.endsWith('$')) {
      throw new Error('Invalid Comparison ' + k)
    }
  }

  return { cmps }
}

function checkError(res: any, reply: any = null) {
  if(res.status ? 
     (null != res.status.code && 0 != res.status.code) : 
       (null != res.code && 0 != res.code)) {

    if(null == reply) { 
      throw new Error(JSON.stringify(res))
    } else {
      reply(new Error(JSON.stringify(res)))
    }
  }
}

// Default options.
const defaults: Options = {
  debug: false,
  map: Any(),
  index: {
    map: {},
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
    address: '0.0.0.0:19530', // HOST:PORT
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
  utils: { },
})

export default MilvusStore

if ('undefined' !== typeof module) {
  module.exports = MilvusStore
}
