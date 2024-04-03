/* Copyright © 2024 Seneca Project Contributors, MIT License. */

require('dotenv').config({ path: '.env.local' })
// console.log(process.env) // remove this
// To turn off milvus-node logs, run: > export NODE_ENV='' && npm run test

import Seneca from 'seneca'
// import SenecaMsgTest from 'seneca-msg-test'
// import { Maintain } from '@seneca/maintain'

import MilvusStoreDoc from '../src/MilvusStoreDoc'
import MilvusStore from '../src/MilvusStore'


describe('MilvusStore', () => {
  test('load-plugin', async () => {
    expect(MilvusStore).toBeDefined()
    expect(MilvusStoreDoc).toBeDefined()
    const seneca = Seneca({ legacy: false })
      .test()
      .use('promisify')
      .use('entity')
      .use(MilvusStore)
    await seneca.ready()

    expect(seneca.export('MilvusStore/native')).toBeDefined()
  })

  /*
  test('utils.resolveIndex', () => {
    const utils = MilvusStore['utils']
    const resolveIndex = utils.resolveIndex
    const seneca = makeSeneca()
    const ent0 = seneca.make('foo')
    const ent1 = seneca.make('foo/bar')

    expect(resolveIndex(ent0, { index: {} })).toEqual('foo')
    expect(resolveIndex(ent0, { index: { exact: 'qaz' } })).toEqual('qaz')

    expect(resolveIndex(ent1, { index: {} })).toEqual('foo_bar')
    expect(resolveIndex(ent1, { index: { prefix: 'p0', suffix: 's0' } })).toEqual('p0_foo_bar_s0')
    expect(resolveIndex(ent1, {
      index: { map: { '-/foo/bar': 'FOOBAR' }, prefix: 'p0', suffix: 's0' }
    }))
      .toEqual('FOOBAR')
  }, 22222)
  */

  test('vector-search', async () => {
    const seneca = await makeSeneca()
    await seneca.ready()
    
    // nearest neighbors
    let knn = 3
    
    for(let i = 10; i >= 0; i--) {
      let ii = i / 10
      await seneca.entity('foo/chunk')
        .make$()
        .data$({
          test_n: 0,
          vector: [
            ii + 0.1,
            ii + 0.2,
            ii + 0.3,
            ii + 0.4,
            ii + 0.5,
            ii + 0.6,
            ii + 0.7,
            ii + 0.8
          ],
          directive$: { vector$: true },
        })
        .save$()
    
    }
    
    // TODO:
    // await seneca.entity('foo/chunk').remove$({ test_n: 0, all$: true })
    
    let list = await seneca.entity('foo/chunk')
      .list$({
        vector: [ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8 ],
        directive$: { vector$: { k: knn } }
      })
    
    // console.log('LIST: ', list)
    
    expect(list.length).toEqual(knn)
    
    list = await seneca.entity('foo/chunk').list$({ test_n: 0 })
    
    expect(list.length).toEqual(11)
    
    // CLEAR
    for(let item of list) {
      // console.log('ITEM: ', item)
      let load_chunk = await seneca.entity('foo/chunk').load$(item.id)
      expect(load_chunk.id).toEqual(item.id)
      let r_chunk = await seneca.entity('foo/chunk').remove$({ id: item.id })
      expect(r_chunk).toBeNull()
    }
    
  }, 22222)
  
  test('insert-remove', async () => {
    const seneca = await makeSeneca()
    await seneca.ready()


    // no query params means no results
    const list0 = await seneca.entity('foo/chunk').list$()
    console.log('list0: ', list0)
    expect(0 === list0.length).toBeTruthy()

    const list1 = await seneca.entity('foo/chunk').list$({ x: 'insert-remove' })
    console.log('list1: ', list1)
    
    let ent0: any

    if (0 === list1.length) {
      ent0 = await seneca.entity('foo/chunk')
        .make$()
        .data$({
          test: 'insert-remove',
          text: 't01',
          vector: [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7],
          directive$: { vector$: true },
        })
        .save$()
      console.log('ent0: ', ent0)
      expect(ent0).toMatchObject({ test: 'insert-remove' })
      await new Promise((r) => setTimeout(r, 2222))
    }
    else {
      ent0 = list1[0]
    }
    
    

    await seneca.entity('foo/chunk').remove$(ent0.id)

    await new Promise((r) => setTimeout(r, 2222))

    const list2 = await seneca.entity('foo/chunk').list$({ test: 'insert-remove' })
    console.log('list2: ', list2)
    expect(list2).toEqual([])
    
  }, 22222)


  test('vector-cat', async () => {
 
    const seneca = await makeSeneca()
    await seneca.ready()

    // const list0 = await seneca.entity('foo/chunk').list$({ test: 'vector-cat' })
    // console.log('list0', list0)

    // NOT AVAILABLE ON AWS
    // await seneca.entity('foo/chunk').remove$({ all$: true, test: 'vector-cat' })

    const list1 = await seneca.entity('foo/chunk').list$({ test: 'vector-cat' })
    // console.log('list1', list1)

    /*
    for (let i = 0; i < list1.length; i++) {
      await list1[i].remove$()
    }

    await new Promise((r) => setTimeout(r, 2222))

    const list1r = await seneca.entity('foo/chunk').list$({ test: 'vector-cat' })
    // console.log('list1r', list1r)
    */
    
    if (!list1.find((n: any) => 'code0' === n.code)) {
      await seneca.entity('foo/chunk')
        .make$()
        .data$({
          code: 'code0',
          test: 'vector-cat',
          text: 't01',
          vector: [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1],
          directive$: { vector$: true },
        })
        .save$()
    }

    if (!list1.find((n: any) => 'code1' === n.code)) {
      await seneca.entity('foo/chunk')
        .make$()
        .data$({
          code: 'code1',
          test: 'vector-cat',
          text: 't01',
          vector: [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1],
          directive$: { vector$: true },
        })
        .save$()
    }

    await new Promise((r) => setTimeout(r, 2222))

    const list2 = await seneca.entity('foo/chunk').list$({
      directive$: { vector$: { k: 2 } },
      vector: [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1],
    })
    // console.log('list2', list2.map((n: any) => ({ ...n })))
    expect(1 < list2.length).toEqual(true)

    const list3 = await seneca.entity('foo/chunk').list$({
      directive$: { vector$: true }, // { k: 2 } },
      vector: [0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1, 0.1],
      code: 'code0'
    })
    console.log('LIST3: ', list3)
    expect(list3.length).toEqual(1)

  }, 22222)



})


function makeSeneca() {
  return Seneca({ legacy: false })
    .test()
    .use('promisify')
    .use('entity')
    .use(MilvusStore, {
      map: {
        'foo/chunk': '*'
      },
      milvus: {
        address: '0.0.0.0:19530',
      }
    })
}


const index_test01 = {
  "mappings": {
    "properties": {
      "text": { "type": "text" },
      "vector": {
        "type": "knn_vector",
        "dimension": 8, // 1536,
        "method": {
          "engine": "nmslib",
          "space_type": "cosinesimil",
          "name": "hnsw",
          "parameters": { "ef_construction": 512, "m": 16 }
        }
      }
    }
  },
  "settings": {
    "index": {
      "number_of_shards": 2,
      "knn.algo_param": { "ef_search": 512 },
      "knn": true
    }
  }
}


/*
  [
  {
    "Rules": [
      {
        "Resource": [
          "collection/podmind03a"
        ],
        "Permission": [
          "aoss:CreateCollectionItems",
          "aoss:DeleteCollectionItems",
          "aoss:UpdateCollectionItems",
          "aoss:DescribeCollectionItems"
        ],
        "ResourceType": "collection"
      },
      {
        "Resource": [
          "index/podmind03a/*"
        ],
        "Permission": [
          "aoss:CreateIndex",
          "aoss:DeleteIndex",
          "aoss:UpdateIndex",
          "aoss:DescribeIndex",
          "aoss:ReadDocument",
          "aoss:WriteDocument"
        ],
        "ResourceType": "index"
      }
    ],
    "Principal": [
      "arn:aws:iam::...:role/...LambdaRole..."
    ],
    "Description": "Easy data policy"
  }
]
  */
