/**
 * Created by root on 21.07.15.
 */
var s3 = require('s3');
var Busboy = require('busboy');
var AWS = require('aws-sdk');
var _ = require('lodash');

var client, Uploader, bucket;

module.exports = ConnectorService;

function ConnectorService(settings) {
    bucket = settings.bucket;
    console.log(bucket);
    client = s3.createClient({
        s3Options: {
            accessKeyId: settings.accessKeyId,
            secretAccessKey: settings.secretAccessKey
        }
    });
    Uploader = require('s3-upload-stream')(new AWS.S3({
        accessKeyId: settings.accessKeyId,
        secretAccessKey: settings.secretAccessKey
    }));
};


ConnectorService.prototype.getFiles = function (folder, cb) {
    var list = client.listObjects({s3Params: {"Bucket": bucket, Prefix: folder + '/'}, recursive: false}, function(err, data){
        if(err) {
            return cb(err);
        }
    });
    list.on('data', function(data) {
        var response = [];
        _.each(data.Contents, function(item){
            response.push({
                Key: item.Key.replace(data.Prefix, '')
            });
        });
        cb(null, response);
    });
    list.on('end', function(data) {
        console.log(list);
    });
};

ConnectorService.prototype.upload = function (container, req, res, cb) {
    var busboy = new Busboy({ headers: req.headers });
    busboy.on('file', function(fieldname, file, filename, encoding, mimetype) {
        uploadS3(file, container + '/' + filename, mimetype, function (err) {
            if (err) {
                cb(err);
            } else {
                cb(null, 'ok');
            }
        });
    });
    req.pipe(busboy);
};

ConnectorService.prototype.delete = function(files, cb) {
    var filesToDel = [];
    _.each(files, function(file){
        filesToDel.push({
            Key: file
        });
    });
    var files = client.deleteObjects({
        Bucket: bucket,
        Delete: {
            Objects: filesToDel
        }
    });
    files.on('end', function(){
        cb(null, 'ok');
    });
};


function uploadS3 (readStream, key, mimetype, callback) {
    var upload = Uploader.upload({
        'Bucket': bucket,
        'Key': key,
        'ContentType': mimetype,
        'ACL':'public-read'
    });
    upload.on('error', function (err) {
        callback(err);
    });
    upload.on('uploaded', function (details) {
        callback(null, details);
    });
    readStream.pipe(upload);
};

ConnectorService.prototype.upload.shared = true;
ConnectorService.prototype.upload.accepts = [
    {arg: 'container', type: 'string'},
    {arg: 'req', type: 'object', 'http': {source: 'req'}},
    {arg: 'res', type: 'object', 'http': {source: 'res'}}
];
ConnectorService.prototype.upload.returns = {arg: 'result', type: 'object'};
ConnectorService.prototype.upload.http =
{verb: 'post', path: '/upload/:container'};


ConnectorService.prototype.getFiles.shared = true;
ConnectorService.prototype.getFiles.accepts = [
    {arg: 'folder', type: 'string'}
];
ConnectorService.prototype.getFiles.returns = {arg: 'files', root: true, type: 'array'};
ConnectorService.prototype.getFiles.http =
{verb: 'get', path: '/getFiles'};

ConnectorService.prototype.delete.shared = true;
ConnectorService.prototype.delete.accepts = [
    {arg: 'files', type: 'array'}
];
ConnectorService.prototype.delete.returns = {arg: 'text', root: true};
ConnectorService.prototype.delete.http =
{verb: 'post', path: '/delete'};
