"use strict";
/* Copyright (c) 2024 Seneca contributors, MIT License */
Object.defineProperty(exports, "__esModule", { value: true });
const milvus2_sdk_node_1 = require("@zilliz/milvus2-sdk-node");
const gubu_1 = require("gubu");
const { Open, Any } = gubu_1.Gubu;
function MilvusStore(options) {
    const seneca = this;
    const init = seneca.export('entity/init');
    let desc = 'MilvusStore';
    let client;
    let store = {
        name: 'MilvusStore',
        save: function (msg, reply) {
            // const seneca = this
            const ent = msg.ent;
            const canon = ent.canon$({ object: true });
            const collection = getCollection(ent, options);
            const body = ent.data$(false);
            // console.log('IN SAVE: ', collection, body)
            const fieldOpts = options.field;
            async function doSave() {
                await createAndLoadCollection(client, {
                    schema: options.milvus.schema,
                    collection: options.milvus.collection,
                    collection_name: collection.name
                });
                client.insert({
                    collection_name: collection.name,
                    data: [body],
                })
                    .then((res) => {
                    let id = res.IDs.int_id.data[0];
                    body.id = id;
                    reply(null, ent.data$(body));
                })
                    .catch((err) => {
                    reply(err, null);
                });
            }
            doSave();
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
        load: function (msg, reply) {
            // const seneca = this
            const ent = msg.ent;
            console.log("LOADDD");
            // const canon = ent.canon$({ object: true })
            const index = resolveIndex(ent, options);
            let q = msg.q || {};
            reply({});
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
        list: function (msg, reply) {
            // const seneca = this
            let vector;
            const q = msg.q || {};
            const ent = msg.ent;
            const index = resolveIndex(ent, options);
            const query = buildQuery({ index, options, msg });
            const collection = getCollection(ent, options);
            vector = q.vector;
            query.vector = vector;
            // console.log('LISTQ')
            // console.dir(query, { depth: null })
            if (null == query) {
                return reply([]);
            }
            console.log('LIST QUERY: ', query);
            async function doList() {
                let res = await client.search(query);
                if (0 != res.status.code) {
                    throw new Error(JSON.stringify(res.status));
                }
                let list = res.results.map((item) => ent.data$(item));
                reply(null, list);
            }
            doList();
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
        remove: function (msg, reply) {
            // const seneca = this
            const ent = msg.ent;
            const index = resolveIndex(ent, options);
            const q = msg.q || {};
            let id = q.id;
            let query;
            if (null == id) {
                query = buildQuery({ index, options, msg });
                if (null == query || true !== q.all$) {
                    return reply(null);
                }
            }
            // console.log('REMOVE', id)
            // console.dir(query, { depth: null })
            reply(null);
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
        close: function (_msg, reply) {
            this.log.debug('close', desc);
            reply();
        },
        // TODO: obsolete - remove from seneca entity
        native: function (_msg, reply) {
            reply(null, {
                client: () => client,
            });
        },
    };
    let meta = init(seneca, options, store);
    desc = meta.desc;
    seneca.prepare(async function () {
        const address = options.milvus.address;
        const token = options.milvus.token;
        client = new milvus2_sdk_node_1.MilvusClient({ address, token });
        console.log(client.createIndex, options.milvus.index);
        let out = await client.createIndex({
            collection_name: 'foo_chunk',
            field_name: 'vector',
            ...options.milvus.index,
        });
        console.log(out);
    });
    return {
        name: store.name,
        tag: meta.tag,
        exportmap: {
            native: () => {
                return { client };
            },
        },
    };
}
function buildQuery(spec) {
    var _a;
    const { index, options, msg } = spec;
    const q = msg.q || {};
    const fields = q.fields$ || [];
    const collection = getCollection(msg.ent, options);
    let query = {
        collection_name: collection.name,
    };
    let index_config = options.milvus.index;
    let outputFields = [...options.milvus.schema.map((field) => field.name), ...fields];
    const parts = [];
    /*
       for (let k in q) {
       if (!excludeKeys[k] && !k.match(/\$/)) {
       parts.push({
       match: { [k]: q[k] },
       })
       }
       }
     */
    const vector$ = msg.vector$ || ((_a = q.directive$) === null || _a === void 0 ? void 0 : _a.vector$);
    if (vector$) {
        query['topk'] = null == vector$.k ? 11 : vector$.k;
    }
    query = {
        ...query,
        ...index_config['searchSettings'],
        output_fields: outputFields
    };
    return query;
}
function resolveIndex(ent, options) {
    let indexOpts = options.index;
    if ('' != indexOpts.exact && null != indexOpts.exact) {
        return indexOpts.exact;
    }
    let canonstr = ent.canon$({ string: true });
    indexOpts.map = indexOpts.map || {};
    if ('' != indexOpts.map[canonstr] && null != indexOpts.map[canonstr]) {
        return indexOpts.map[canonstr];
    }
    let prefix = indexOpts.prefix;
    let suffix = indexOpts.suffix;
    prefix = '' == prefix || null == prefix ? '' : prefix + '_';
    suffix = '' == suffix || null == suffix ? '' : '_' + suffix;
    // TOOD: need ent.canon$({ external: true }) : foo/bar -> foo_bar
    let infix = ent
        .canon$({ string: true })
        .replace(/-\//g, '')
        .replace(/\//g, '_');
    return prefix + infix + suffix;
}
function getCollection(ent, options) {
    let collection = {};
    let canon = ent.canon$({ object: true });
    collection.name = (null != canon.base ? canon.base + '_' : '') + canon.name;
    return collection;
}
async function createAndLoadCollection(client, config) {
    const collection = config.collection || {};
    const collection_name = config.collection_name || '';
    const schema = config.schema || [];
    let coll;
    coll = await client.createCollection({
        collection_name,
        fields: schema,
        ...collection,
    });
    coll = await client.loadCollection({
        collection_name,
        ...collection,
    });
}
// Default options.
const defaults = {
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
                data_type: milvus2_sdk_node_1.DataType.Int64,
                is_primary_key: true,
                autoID: true,
            },
            {
                name: 'vector',
                description: 'Vector field',
                data_type: milvus2_sdk_node_1.DataType.FloatVector,
                dim: 8,
            },
        ],
        collection: {
            consistency_level: milvus2_sdk_node_1.ConsistencyLevelEnum.Strong,
            enable_dynamic_field: true,
        },
        index: {
            index_type: 'HNSW',
            params: { efConstruction: 512, M: 16 },
            metric_type: 'COSINE',
            searchSettings: {
                params: { ef: 512 },
                consistency_level: milvus2_sdk_node_1.ConsistencyLevelEnum.Strong,
                metric_type: 'COSINE',
            }
        }
    }),
};
Object.assign(MilvusStore, {
    defaults,
    utils: { resolveIndex },
});
exports.default = MilvusStore;
if ('undefined' !== typeof module) {
    module.exports = MilvusStore;
}
//# sourceMappingURL=MilvusStore.js.map