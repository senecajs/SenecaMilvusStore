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
            const q = msg.q || {};
            const collection_name = makeCollectionName(ent.canon$({ string: true }));
            const body = ent.data$(false);
            const id = q.id || ent.id;
            // console.log('IN SAVE: ', collection, body)
            const fieldOpts = options.field;
            async function doSave() {
                await loadCollection(client, {
                    collection_name,
                    collection: options.milvus.collection,
                });
                if (null == id) {
                    let res = await client.insert({
                        collection_name,
                        data: [body],
                    });
                    checkError(res, reply);
                    let id = res.IDs.int_id.data[0];
                    body.id = id;
                    reply(null, ent.make$(body));
                }
                else {
                    // console.log("IN UPSERT", body)
                    reply(new Error("UPSERT NOT SUPPORTED"), null);
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
            doSave();
        },
        load: function (msg, reply) {
            // const seneca = this
            const ent = msg.ent;
            let collection_name = makeCollectionName(ent.canon$({ string: true }));
            let query = buildQuery({ options, msg });
            let q = msg.q || {};
            // console.log('IN LOAD: ', collection_name, query, options.milvus.collection)
            async function doLoad() {
                await loadCollection(client, {
                    collection_name,
                    collection: options.milvus.collection,
                });
                let res = await client.get({
                    collection_name,
                    ids: [q.id],
                    output_fields: query.output_fields,
                });
                checkError(res, reply);
                if (null == res.data[0]) {
                    return reply(null);
                }
                reply(null, ent.make$(res.data[0]));
            }
            doLoad();
        },
        list: function (msg, reply) {
            // const seneca = this
            const q = msg.q || {};
            const ent = msg.ent;
            const query = buildQuery({ options, msg });
            const collection_name = makeCollectionName(ent.canon$({ string: true }));
            const vector = q.vector;
            if (null == query) {
                return reply([]);
            }
            async function doList() {
                // Load collection in memory
                await loadCollection(client, {
                    collection_name,
                    collection: options.milvus.collection,
                });
                if (vector) {
                    query.vector = vector;
                    let res = await client.search(query);
                    console.log('LIST SEARCH: ', query);
                    // console.dir(res, { depth: null })
                    checkError(res, reply);
                    let list = res.results.map((item) => ent.make$().data$(item));
                    return reply(null, list);
                }
                else {
                    let cq = seneca.util.clean(q);
                    let expr = Object.keys(cq).map(c => {
                        return build_cmps(cq[c], c).cmps.map(cmp => {
                            return cmp.k + cmp.cmpop + JSON.stringify(cmp.v);
                        }).join('and');
                    }).join('or');
                    let filter_query = {
                        collection_name,
                        limit: 100,
                        expr,
                        output_fields: query.output_fields,
                    };
                    console.log('EXPR: ', filter_query, [expr, cq]);
                    let res = await client.query(filter_query);
                    checkError(res, reply);
                    reply(res.data);
                    // let res: any = {}
                    // reply(res)
                    console.log("IN LIST QUERY: ", filter_query, q, res);
                    // reply(res.data)
                }
            }
            doList();
        },
        // NOTE: all$:true is REQUIRED for deleteByQuery
        remove: function (msg, reply) {
            // const seneca = this
            const ent = msg.ent;
            const q = msg.q || {};
            let id = q.id;
            let query;
            if (null == id) {
                query = buildQuery({ options, msg });
                if (null == query || true !== q.all$) {
                    return reply(null);
                }
            }
            // console.log('REMOVE', id)
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
        console.log("IN PREPARE: ", address, token);
        client = new milvus2_sdk_node_1.MilvusClient({ address, token });
        // console.log('IN PREPARE: ', client.createIndex, options.milvus.index, options.map)
        for (let canon in options.map) {
            let res;
            let collection_name = makeCollectionName(canon);
            let collection_exists = await client.hasCollection({ collection_name });
            checkError(collection_exists);
            if (collection_exists.value) {
                continue;
            }
            res = await client.createCollection({
                collection_name,
                fields: options.milvus.schema,
                ...options.milvus.collection,
            });
            checkError(res);
            // console.log('IN COLL: ', res)
            res = await client.createIndex({
                collection_name,
                field_name: 'vector', // TODO: FEATURE TO INDEX OTHER FIELDS
                ...options.milvus.index,
            });
            checkError(res);
            // console.log('IN INDEX: ', res)
        }
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
function makeCollectionName(canon) {
    let [zone, base, name] = canon.split('/');
    zone = '-' == zone ? '' : zone;
    base = '-' == base ? '' : base;
    name = '-' == name ? '' : name;
    let str = [zone, base, name].filter((v) => null != v && '' != v).join('_');
    return str;
}
function buildQuery(spec) {
    var _a;
    const { options, msg } = spec;
    const q = msg.q || {};
    const fields = q.fields$ || [];
    const collection_name = makeCollectionName(msg.ent.canon$({ string: true }));
    // no query params means no results
    if (0 === Object.keys(q).length) {
        return null;
    }
    let query = {
        collection_name,
    };
    let index_config = options.milvus.index;
    let outputFields = [...options.milvus.schema.map((field) => field.name), ...fields];
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
async function loadCollection(client, config) {
    const collection_name = config.collection_name || '';
    const collection = config.collection || {};
    let res;
    res = await client.loadCollection({
        collection_name,
        ...collection,
    });
    checkError(res);
}
function build_cmps(qv, kname) {
    // console.log('QV: ', typeof qv, qv)
    if ('object' != typeof qv) {
        //  && !Array.isArray(qv)) {
        return { cmps: [{ c: 'eq$', cmpop: '==', k: kname, v: qv }] };
    }
    let cmpops = {
        gt$: { cmpop: '>' },
        gte$: { cmpop: '>=' },
        lt$: { cmpop: '<' },
        lte$: { cmpop: '<=' },
        ne$: { cmpop: '!=' },
        eq$: { cmpop: '==' },
    }, cmps = [];
    for (let k in qv) {
        let cmp = cmpops[k];
        if (cmp) {
            cmp = { ...cmpops[k] };
            cmp.k = kname;
            cmp.v = qv[k];
            cmp.c = k;
            cmps.push(cmp);
        }
        else if (k.endsWith('$')) {
            throw new Error('Invalid Comparison ' + k);
        }
    }
    return { cmps };
}
function checkError(res, reply = null) {
    if (res.status ?
        (null != res.status.code && 0 != res.status.code) :
        (null != res.code && 0 != res.code)) {
        if (null == reply) {
            throw new Error(JSON.stringify(res));
        }
        else {
            reply(new Error(JSON.stringify(res)));
        }
    }
}
// Default options.
const defaults = {
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
    utils: {},
});
exports.default = MilvusStore;
if ('undefined' !== typeof module) {
    module.exports = MilvusStore;
}
//# sourceMappingURL=MilvusStore.js.map