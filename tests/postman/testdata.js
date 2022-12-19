if(!process.env.APIKEY){
  throw "API KEY in environment variable APIKEY required"
}
const HOST = "http://localhost:3004"
const APIKEY = `Bearer ${process.env.APIKEY}`


const swagger = require(" ../../docs/swagger.json")

var EstuaryClient = require('estuary-client');
let defaultClient = EstuaryClient.ApiClient.instance;
var bearerAuth = defaultClient.authentications['bearerAuth'];
bearerAuth.apiKey = APIKEY

defaultClient.basePath = HOST
let opts = {
  'expiry': "24h"
};


async function genApiKey(){
  return new Promise((resolve, reject) => {
    let apiInstance = new EstuaryClient.UserApi();
    apiInstance.userApiKeysPost(opts, (error, data, response) => {
      if (error) {
        reject(error)
      } else {
        resolve(data)
      }
    });
  })
}


async function genCollection(){
  return new Promise((resolve, reject) => {
  let apiInstance = new EstuaryClient.CollectionsApi();
  let body = new EstuaryClient.MainCreateCollectionBody({"name":"NAME","description":"DESCRIPTION"}); // MainCreateCollectionBody | Collection name and description
  apiInstance.collectionsPost(body, (error, data, response) => {
    if (error) {
      reject(error)
    } else {
      resolve(data)
    }
  });
})


}

async function init(){
  //for api key delete
  const apiKey = await genApiKey()
  data['key_or_hash'] = apiKey.token 
  data['coluuid'] = []
  for(var i = 0; i < 9; i++){
    collection = await genCollection()
    data['coluuid'].push(collection['uuid'])
  }
  data['#/definitions/util.ContentAddIpfsBody'] = {"root":cid, "filename": filename, "coluuid": data['coluuid'].pop() }
  data['#/definitions/main.importDealBody'] = {"coluuid": data['coluuid'].pop(), "name": name, "dealIDs" : dealIDs }
  data['#/definitions/main.importDealBody'] = {"coluuid": data['coluuid'].pop(), "name": name, "dealIDs" : dealIDs }
  for(var def in swagger.definitions){
    var value = {}
    if(swagger.definitions[def].properties){
      for(var key in swagger.definitions[def].properties){
        value[key] = get_value(key)
      }
    }
    data['#/definitions/'+def] = value
  }
}


var data = {}
const body = '{}'
const cid = "bafkreifvxooyaffa7gy5mhrb46lnpdom34jvf4r42mubf5efbodyvzeujq"
const content_id = "1"
const dealid = "1" 
const empty = ""
const filename="/testfile"
const apikey="foo" 
const name="testname"
const pinid = 1 
const propcid = 1 
const chanid = ""
const ContentCreateBody = {} 
const pubkey = "CAESIKFRR4yzxwk1aKotoPiUtR8OeQ/yydF18J9h5Y8gLAFN"
const peerId = "12D3KooWN8vAoGd6eurUSidcpLYguQiGZwt4eVgDvbgaS7kiGTup"
const addresses = ["/ip4/127.0.0.1/tcp/4001/p2p/12D3KooWLg5h6dARgiBcY6yTTMw8ivPzJufjEBDXahJNKWRGMWNg"]
const data_filename = 'foo' // what we're using as a test file

const dealIDs = [] 

const miner="f02620"

const dealbodysize = 100
const dealbodyreplication = 1
const dealbodydurationBlks = 100
const dealbodyverified = true

data['#/definitions/main.createCollectionBody'] = {"name":"name","description":"description"}
data['#/definitions/main.deleteContentFromCollectionBody'] = {"By":"content_id", "Value": content_id}
data['#/definitions/main.estimateDealBody'] = { "size": dealbodysize, "replication" : dealbodyreplication, "durationBlks": dealbodydurationBlks, "verified": dealbodyverified }

data['addresses_url'] = addresses[0]
data['pubKey_url'] = pubkey


data["ID"] = peerId
data['Addrs'] = addresses
data['Connected'] = true
data['addresses'] = addresses
data['all'] = empty
data['begin'] = empty
data['body'] = cid
data['chanid'] = chanid
data['cid'] = cid
data['cont'] = content_id
data['content'] = content_id
data['contentIDs'] = [content_id]
data['content_id'] = content_id
data['contentid'] = content_id
data['data'] = data_filename
data['datacid'] = cid
data['deal'] = dealid
data['dealRequest'] =  {"content_id": content_id}
data['dealid'] = dealid
data['dir'] =  empty
data['duration'] = empty
data['expiry'] = '24h'
data['filename'] = filename
data['id'] =  1 
data[':id'] =  1 
data['ignore-dupes'] = empty
data['ignore-failed'] = empty
data['key'] =  apikey
data['lazy-provide'] =  empty
data['limit'] = empty
data['miner'] = miner 
data['name'] = name
data['offset'] = empty
data['path'] = filename
data['peerIds'] = [peerId]
data['perms'] =  empty
data['pinid'] = pinid
data['propcid'] = propcid
data['pubKey'] = pubkey
data['replication'] = empty
data['req'] = ContentCreateBody
data['root'] = cid
data['origins'] = addresses
data['meta'] = {}
data['key_or_hash'] = empty
data['pin'] = {"cid":cid, "name":name}


function get_value(key){
  if(key == "coluuid"){
    return data[key].pop()
  }
  return data[key]
}
module.exports = { data , init, get_value};

