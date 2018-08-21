var fs = require('fs');
var stringHash = require('@sindresorhus/string-hash');
var chunklen=4096;
var _map=new Map();
var _delarr=[];
function savemap(callback)
{
var len=_map.size*8+4+_delarr.length*4;
var w_data = Buffer.alloc(len);
console.log(_map.size+": size");
console.log(len+": len");
w_data.writeInt32LE(_map.size);
var index=0;
for(var [key,value] of _map)
{ 
    console.log(key+":"+value);
    w_data.writeInt32LE(key,index*8+4);
    w_data.writeInt32LE(value,index*8+8);
    index++;
}
index=0;
var pos=_map.size*8+4;
for(var value of _delarr)
{
    console.log("_delarr val="+value);
    w_data.writeInt32LE(value,pos+index*4);
    index++;
}
//console.log("bytes: " + w_data.toString('hex'));
fs.open('./hmdb.ind','w',function(err,fd){
    fs.write(fd,w_data,0,len,0,function(err,written,buffer){
        if(err) console.log('写文件操作失败');
        console.log('写文件操作成功');
        callback(null, "OK")
    });
});
}
function loadmap(callback)
{
   fs.readFile('./hmdb.ind',function (err, str) {
      if (err) {
         console.log(err);
         callback(err, "FAIL")
      }else {
         var buf = Buffer.from(str);
         console.log(buf.length);
         var size=buf.readInt32LE(0);
         console.log("loadmap size="+size);
         for(var i=0;i<size;i++)
         {
          var th=buf.readInt32LE(i*8+4);
          console.log("loadmap hash="+th);
          var th2=buf.readInt32LE(i*8+8);
          console.log("loadmap seek="+th2);
          _map.set(th, th2);
         }
         var pos=size*8+4;
         var arrlen=buf.length-pos;
         console.log("arrlen="+arrlen);
         for(var i=0;i<arrlen/4;i++)
         {
            var delind=buf.readInt32LE(pos+i*4);
            console.log("delind="+delind);
         }
         callback(null, "OK")
      }
   });
}
function putdata(key,data,callback){
var code = stringHash(key);
if(!_map.has(code)){
if(_delarr.length==0){
//addfile
var seek=_map.size*chunklen;
_map.set(code, seek);
wfile(code,data,seek,callback);
}else{
//changefile
var seek=_delarr.pop();
wfile(code,data,seek,callback);
}
}else
{
//changefile
var seek=_map.get(code);
wfile(code,data,seek,callback);
}
}
function getdata(key,callback){
var code = stringHash(key);
if(_map.has(code)){
//getfile
var seek=_map.get(code);
rfile(code,seek,callback);
}else
{
callback(null, "FAIL")
}
}
function deldata(key,callback)
{
var code = stringHash(key);
if(_map.has(code)){
var seek=_map.get(code);
//向delarr添加数据
_delarr.push(seek);
//删除此hash
_map.delete(code);
callback(null, "OK")
}else
{
callback(null, "FAIL")
}
}
function wfile(hash,data,seek,callback)
{
console.log("wfile seek="+seek);
var w_data = Buffer.alloc(chunklen);
w_data.writeInt32LE(hash);
w_data.writeInt32LE(data.length,4);
w_data.write(data, 8);
/*console.log("bytes: " + w_data.toString('hex', 0, 4096));
var th=w_data.readInt32LE();
console.log("hash="+th);
var th2=w_data.readInt32LE(4);
console.log("len="+th2);
var th3=w_data.toString('utf8',8,8+th2);
console.log("str="+th3);*/
//将此buffer写入文件
fs.open('./hmdb.db','w',function(err,fd){
    fs.write(fd,w_data,0,chunklen,seek,function(err,written,buffer){
        if(err) console.log('写文件操作失败');
        console.log('写文件操作成功');
        callback(null, "OK")
    });
});
}
function rfile(hash,seek,callback)
{
//读取文件内容到buffer
fs.open('./hmdb.db','r',function(err,fd){
    console.log("rfile seek="+seek);
    var w_data = Buffer.alloc(chunklen);
    fs.read(fd,w_data,0,chunklen,seek,function(err,written,buffer){
        if(err) console.log('读取文件操作失败');
        console.log('读取文件操作成功');
        var th=w_data.readInt32LE();
        console.log("hash="+th);
        var th2=w_data.readInt32LE(4);
        console.log("len="+th2);
        var th3=w_data.toString('utf8',8,8+th2);
        console.log("rstr="+th3);
        callback(null, th3)
    });
});
}
function rfile2(hash,seek)
{
console.log("rfile seek="+seek);
var w_data = Buffer.alloc(chunklen);
//读取文件内容到buffer
var fd = fs.openSync('./hmdb.db', 'r');
var content=fs.readSync(fd, w_data,0,chunklen,seek);
console.log("content="+content);
var th=w_data.readInt32LE();
console.log("hash="+th);
var th2=w_data.readInt32LE(4);
console.log("len="+th2);
var th3=w_data.toString('utf8',8,8+th2);
console.log("rstr="+th3);
fs.closeSync(fd);
return th3;
}
putdata('1','100',function (err, content) {
    console.log("putdata="+content)
});
deldata('1',function (err, content) {
    console.log("deldata="+content)
});
putdata('2','200',function (err, content) {
    console.log("putdata="+content)
});

putdata('1','300',function (err, content) {
    console.log("putdata="+content)
});
getdata('1',function (err, content) {
    console.log("getdata="+content)
});
getdata('2',function (err, content) {
    console.log("getdata="+content)
});

savemap(function (err, content) {
    console.log("savemap="+content)
});
loadmap(function (err, content) {
    console.log("loadmap="+content)
});
