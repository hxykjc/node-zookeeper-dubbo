/**
 * Created by panzhichao on 16/8/2.
 */
'use strict';
const net       = require('net');
const url       = require('url');
const zookeeper = require('node-zookeeper-client');
const qs        = require('querystring');
const reg       = require('./libs/register');
const decode    = require('./libs/decode');
const Encode    = require('./libs/encode').Encode;
let Java = null; //require('js-to-java');

require('./utils');

/**
 * @param {Object} opt
 * @constructor
 */

var NZD = function (opt) { 
  EventEmitter.call(this);
  const self       = this;
  this.dubboVer    = opt.dubboVer;
  this.application = opt.application;
  this.group       = opt.group;
  this.timeout     = opt.timeout || 6000;
  this._root        = opt.root || 'dubbo';
  this.dependencies = opt.dependencies || {};
  this.client       = zookeeper.createClient(opt.register, {
      sessionTimeout: 30000,
      spinDelay     : 1000,
      retries       : 5
  });

  this.client.connect();
  this.client.once('connected', function () {
      self._applyServices();
      self._consumer();
  });
  // 由 node-zookeeper-client 包控制
  this.client.on("disconnected", function() {
      Logger.info("zookeeper 连接失败或连接中断，正在尝试重新连接")
  })
};
util.inherits(NZD, EventEmitter);

NZD.prototype._consumer = reg.consumer;
NZD.prototype._applyServices = function () {
  const refs = this.dependencies;
  const self = this;

  for (const key in refs) { 
      NZD.prototype[key] = new Service(self.client, self.dubboVer, refs[key], self);
  }
};

var Service = function (zk, dubboVer, depend, opt) {
  EventEmitter.call(this);  
  this._zk        = zk;
  this._hosts     = [];
  this._paths     = [];
  this._host      = "";
  this._version   = depend.version;
  this._group     = depend.group || opt.group;
  this._interface = depend.service;
  this._signature = Object.assign({}, depend.methodSignature);
  this._root      = opt._root;

  this._encodeParam = {
      _dver     : dubboVer || '3.0.4',
      _interface: depend.service,
      _version  : depend.version,
      _group    : depend.group || opt.group,
      _timeout  : depend.timeout || opt.timeout,
      _path     : depend.service
  }

  var _this = this;
  this._find(depend.service, function(option) {
      option = option || {};
      if (option.success) {
          Logger.info(`load ${depend.service} success`);
      } else {
          Logger.error(`load ${depend.service} failed`);
      }
      opt.emit("loadservice", option)
  });
};
util.inherits(Service, EventEmitter);

Service.prototype._find = function (path, callback) {
  if (typeof callback !== 'function') {
      callback = function() {};
  }

  const self  = this;
  self._hosts = [];
  self._paths = [];
  self._host = "";
  this._zk.getChildren(`/${this._root}/${path}/providers`, watch, handleResult);

  function watch(event) {
      Logger.info(`service ${path} may offline, try to find ${path} again`);    
      self._find(path)
  }

  function handleResult(err, children) {
      let zoo;
      let isGeneration = false;
      if (err) {
          callback();     
          return Logger.error(err);
      }
      if (children && !children.length) {
          callback();
          return Logger.error(`can't find  the child z-node: ${path} ,pls check dubbo service!`);
      }
      
      for (let i = 0, l = children.length; i < l; i++) {
          zoo = qs.parse(decodeURIComponent(children[i]));
          // 没有显式提供 version 则会添加 default.version 为 1.0.0
          if ((zoo.version === self._version || zoo["default.version"] === self._version) && zoo.group === self._group) {
              let urlParse = url.parse(Object.keys(zoo)[0]);
              self._hosts.push(urlParse.host);
              
              // 可能存在实际服务地址与Z-NODE节点名称不同的情况，如注册同名接口，则实际服务地址会有变化
              let pathname = (urlParse.pathname && urlParse.pathname.slice(1)) || ""
              self._paths.push(pathname)

              // 泛化调用相当于调用 invoke 方法，传参为 methods, argTypes, args
              if (zoo.methods === "*") {
                  zoo.methods = "$invoke"
                  isGeneration = true;
              }
              const methods = zoo.methods.split(',');  
              for (let i = 0, l = methods.length; i < l; i++) {
                  self[methods[i]] = (function (method) {
                      return function () {
                          var args = Array.from(arguments);
                          return self._execute(method, args);
                      };
                  })(methods[i]);
              }
          }
      }

      // 有 Z-NODE 节点，但是节点上不存在服务，调用 API 时做异常处理
      if (!self._hosts.length) {
          callback();      
          return Logger.error(`can't find  the host: ${path} has no provider ,pls check dubbo service!`);
      }

      callback({success: true, isGeneration: isGeneration, service: self._interface});      
      return;
  }
};

// Service.prototype._flush = function (cb) {
//   this._find(this._interface, cb)
// };

Service.prototype._execute = function (method, args) {
  const self                = this;
  const render              = Math.random() * self._hosts.length | 0;  

  this._host                = self._hosts[render].split(':');

  // 如果存在该 host 下的 path，则使用，否则以接口名为 path
  if (self._paths[render]) {
      this._encodeParam._path = self._paths[render];
  } else {
      this._encodeParam._path = self._interface;
  }
  
  this._encodeParam._method = method;
  this._encodeParam._args   = args;
  const buffer              = new Encode(this._encodeParam);
  return new Promise(function (resolve, reject) {
      if (self._hosts.length === 0) {
        return reject(`${self._interface} provider can not be found, pls check`)
      }
      
      const client = new net.Socket();
      const host = self._host;
      
      const chunks = [];
      let heap;
      let head = [];
      let bl = 16;

      client.connect(host[1], host[0], function () {
          client.write(buffer);
      });

      client.on('error', function (err) {
        return reject('service not available, pls try later');
        // self._flush(function () {
        //   host = self._hosts[Math.random() * self._hosts.length | 0].split(':');
        //   client.connect(host[1], host[0], function () {
        //     client.write(buffer);
        //   });
        // })
      });

      client.on('data', function (chunk) {
          if (!head.length && !chunks.length) {
              // 前 16 位是头部信息， 前两字节默认是 Oxdabb，指 dubbo 协议
              head = Array.prototype.slice.call(chunk.slice(0, 16));
              let i   = 0;

              // 长度后 3 位，表明 body 体长度
              while (i < 3) {
                  bl += head.pop() * Math.pow(256, i++);
              }
          }

          chunks.push(chunk);
          heap = Buffer.concat(chunks);
          (heap.length >= bl) && client.destroy();
      });

      client.on('close', function (err) {
          if (!err) {
              decode(heap, function (err, result) {
                  if (err) {
                    return reject(err);
                  }
                  return resolve(result);
              })
          } else {
              Logger.error("socket close error, response ")
              return reject(err)
          }
      });
  });
};

module.exports = NZD;

