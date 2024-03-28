require('dotenv').config({ path: '.env.local' })
// console.log(process.env) // remove this

const Seneca = require('seneca')

run()

async function run() {
  const seneca = Seneca({ legacy: false })
    .test()
    .use('promisify')
    .use('entity')
    .use('..', {
      map: {
        'foo/chunk': '*',
      },
      milvus: {
        address: '0.0.0.0:19530',
        // token: '',
      },
    })

  await seneca.ready()

  // console.log(await seneca.entity('bar/qaz').data$({q:1}).save$())

  const save0 = await seneca.entity('foo/chunk')
        .make$()
        .data$({
          x:3,
          o:{m:'M2',n:3}, 
          text: 't03',
          vector: [0.0,0.1,0.2,0.3,0.4,0.5,0.6,0.6],
          directive$:{vector$:true},
        })
        .save$()
  console.log('save0', save0)


  const list0 = await seneca.entity('foo/chunk').list$({
    directive$:{vector$:true},
    vector:[0.1,0.1,0.2,0.3,0.4,0.5,0.6,0.7],
  })
  console.log('list0', list0.length)

  const list1 = await seneca.entity('foo/chunk').list$({
    directive$:{vector$: { k: 10000 }},
    vector:[0.1,0.1,0.2,0.3,0.4,0.5,0.6,0.7],
    fields$: ['x', 'text'],
  })
  console.log('list1', list1.length)
  // console.log(list1)

  const id = '448691898059868766'
  const load0 = await seneca.entity('foo/chunk').load$(id)
  console.log('load0', load0)

  // console.log(await seneca.entity('bar/qaz').list$())
}
