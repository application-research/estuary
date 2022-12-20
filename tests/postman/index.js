var path = require('path');
const swagger_data = require("../../docs/swagger.json")

var testdata = require("./testdata").data
var testdata_module = require("./testdata")

const HOST = "http://localhost:3004"


if(!process.env.APIKEY){
  throw "API KEY in environment variable APIKEY required"
}
const APIKEY = `Bearer ${process.env.APIKEY}`

var langs = [ {language: "nodejs", variant: "axios", folder: "jstests", extension: ".js"}]

var codegen = require('postman-code-generators'),
  sdk = require('postman-collection'),
  options = {
    indentCount: 2,
    indentType: 'Space',
    trimRequestBody: true,
    followRedirect: true
  };

var fs = require('fs'),

Converter = require('openapi-to-postmanv2'),
//openapiData = fs.readFileSync('openapi.json', {encoding: 'UTF8'});
openapiData = fs.readFileSync("../../docs/swagger.json", {encoding: 'UTF8'});
//swagger.json


function write_file(filename, data){
  fs.writeFileSync(filename, data);
}

function write_file_curl(filename, data){
  data  = data + ` --fail --no-progress-meter`
  fs.writeFileSync(filename, data);
}

function update_request(request, auth){
  request.url.host = [HOST]
  request.url.auth = auth

  return request
}

function process_request(item, auth){
  var request = item.request

  var name = request.url.path.join("_").replace(":","") + "_" + request.method

  request = add_auth_to_header(request, auth)
  request = add_data_to_request(request, item)
  request = new sdk.Request(request)
  request = update_request(request, auth)
  for( var lang_index in langs){
    var lang = langs[lang_index]
    var language = lang.language
    var variant = lang.variant
    var folder = lang.folder
    var extension = lang.extension
    codegen.convert(language, variant, request, options, function(error, snippet){
      process_snippet(error, snippet, name, folder, extension, language)
    })
  }
}

function process_snippet(error, snippet, name, folder, extension, language){
  if (error) {
    console.log("error",error)
  }
  if(language == "curl"){
    write_file_curl(folder + "/"  + name + extension, snippet)
  }else{
    write_file(folder + "/"  + name + extension, snippet)
  }
}

function add_auth_to_header(request, auth){
  if(auth && auth.apikey){
    request.header.push({key: "Authorization", value: APIKEY})
  }
  return request;
}


function get_value(key){
  return testdata_module.get_value(key)
}


function process_variable(request){
  for(index in request.url.variable){
    var variable = request.url.variable[index]
    variable['value'] = get_value(variable['key'])
  }
  return request
}

function process_path(request){
  for(index in request.url.path){
    var query = request.url.path[index]
    if(query[0] == ":"){
      request.url.path[index] = get_value( query.substr(1,query.length-1))
    }
  }
  return request
}

function process_query(request){
  for(index in request.url.query){
    var query = request.url.query[index]
    query['value'] = get_value(query['key'])
  }

  //for format where url has something like /content/add/:filename
  for(index in request.url.query){
    var query = request.url.query[index]
    if(query[0] == ":"){
      query['value'] = get_value( query.substr(1,query.length-1))
    }
  }
  return request
}



function read_swagger(request){
    var paths = []
    for(var index in request['url']['path']){
        const item = request.url.path[index]
        if(item[0] == ":"){
            paths.push("{" + item.substr(1,item.length-1) + "}")
        }else{
            paths.push(item)
        }
    }
    path = "/" + paths.join("/")
    method = request.method.toLowerCase()
    return swagger_data['paths'][path][method]
}


function debug(data){
    console.log(JSON.stringify(data, null, 2))
}

function process_body_form(request, item){
  if(request && request.body && request.body.mode && request.body.mode == "formdata"){
    var body = request.body.formdata
		for(var index in body){
      var key = body[index].key
      if(key == "data"){ // file uploads
        request.body.formdata[index].src = get_value(key) 
      }else{
        request.body.formdata[index].value = get_value(key) 
      }
    }
  }
  return request
}
function process_body_urlencoded(request, item){
  if(request && request.body && request.body.mode && request.body.mode == "urlencoded"){
    var body = request.body.urlencoded
  debug({"body":body})
/*
  "body": [
    {
      "disabled": false,
      "key": "addresses",
      "value": "<string>",
      "description": "(Required) Autoretrieve's comma-separated list of addresses"
    },
    {
      "disabled": false,
      "key": "pubKey",
      "value": "<string>",
      "description": "(Required) Autoretrieve's public key"
    }
  ]
*/
		for(var index in body){
      var key = body[index].key
      request.body.urlencoded[index].value = get_value(key+"_url") // the autoretrieve init endpoint is the only thing currently using this data format and uniquely needs a comma separately list for addresses
    }
  }
  return request
}
function process_body(request, item){
  /*  "body": {
    "mode": "raw",
    "raw": "{\n  \"coluuid\": \"<string>\",\n  \"dir\": \"<string>\",\n  \"filename\": \"<string>\",\n  \"peers\": [\n    \"<string>\",\n    \"<string>\"\n  ],\n  \"root\": \"<string>\"\n}",
    "options": {
      "raw": {
        "language": "json"
      }
    }
  }*/

  if(request && request.body && request.body.mode && request.body.mode == "raw"){
    var swag = read_swagger(request)
    for( var param_index in swag.parameters ){
      var param = swag.parameters[param_index]
      if(param.schema){
        var key = param.schema['$ref']
        if(!key && param.schema.items){
          key = param.schema.items['$ref']
        }
        var data =  get_value(key)
        if(data){
          request.body.raw = JSON.stringify(data)
        }else{
          if(param.schema.type == "string")
          {
            var key = param.name
            var data =  get_value(key)
            request.body.raw = JSON.stringify(data)

          }else if( param.schema.type == "array"){
            var key = param.name
            var data =  get_value(key)
            request.body.raw = JSON.stringify(data)
          }

          else{
            var body_data = {}
            var body = JSON.parse(request.body.raw)
            for(var body_key in body){
              body_data[body_key] = get_value(body_key)
            }
            request.body.raw = JSON.stringify(body_data)
          }

        }
      }else{
        //console.log(request)
        //console.log(param)

      }
    }
  }
  return request
}

function add_data_to_request(request, item){
  request = process_query(request)
  request = process_variable(request)
  request = process_body(request, item)
  request = process_body_urlencoded(request, item)
  request = process_body_form(request, item)
  request = process_path(request)
  return request

}

function process_collections(collections, auth){
  if(collections.item ){
      for (let i = 0; i < collections.item.length; i++) {
        var item = collections.item[i]
        if(item.request){
          process_request(item, auth)
        }else{
          process_collections(item, auth)
        }
      }
  }
}



async function main(){
  await testdata_module.init()
  Converter.convert({ type: 'string', data: openapiData },
    {}, (err, conversionResult) => {
      if (!conversionResult.result) {
        console.log('Could not convert', conversionResult.reason);
      }
      else {
        var auth = conversionResult.output[0].data.auth
        var collections = conversionResult.output[0].data

        process_collections(collections, auth)

      }
    }
  );
}


main()
