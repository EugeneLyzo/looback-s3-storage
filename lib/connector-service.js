/**
 * Created by root on 21.07.15.
 */
var s3 = require( 's3' );
var Busboy = require( 'busboy' );
var AWS = require( 'aws-sdk' );
var _ = require( 'lodash' );
var async = require( 'async' );
var Q = require( 'q' );
var request = require( 'request' );
var path = require( 'path' );

var client, Uploader, bucket;

module.exports = ConnectorService;

function ConnectorService( settings ) {
    bucket = settings.bucket;
    client = s3.createClient( {
        s3Options: {
            accessKeyId: settings.accessKeyId,
            secretAccessKey: settings.secretAccessKey
        }
    } );
    Uploader = require( 's3-upload-stream' )( new AWS.S3( {
        accessKeyId: settings.accessKeyId,
        secretAccessKey: settings.secretAccessKey
    } ) );
};

function listObjects(from){
    return Q.Promise(function(resolve,reject){
        var list = client.listObjects( {
            s3Params: { "Bucket": bucket, Prefix: from + '/' },
            recursive: false
        }, function ( err, data ) {
            if ( err ) {
                return cb( err );
            }
        } );
        list.on( 'data', function ( data ) {
            resolve (data);
        })
    })
}

function deleteDir(to){
    return Q.Promise(function(resolve,reject){
        var del=client.deleteDir( {
            Bucket: bucket,
            Prefix: (to + "/" )
        });
        del.on('end',function(){
            resolve();
        })
    });
}

ConnectorService.prototype.copyDir = function ( from, to, cb ) {

    var params = {
        "Bucket": bucket

    };
    deleteDir(to ).then(function() {
        listObjects( from ).then( function ( data ) {
            async.each( data.Contents, function ( item, callback ) {
                var copyObject = client.copyObject( {
                    Bucket: bucket,
                    Key: (to + "/" + item.Key.replace( data.Prefix, '' )),
                    CopySource: (bucket + '/' + item.Key),
                    'ACL': 'public-read'
                }, function ( err, data ) {
                    if ( err ) {
                        return callback( err );
                    }
                } );
                copyObject.on( 'end', function ( data ) {
                    callback( null );

                } );
                copyObject.on( 'error', function ( data ) {
                    callback( data );
                } );

            }, function ( err ) {
                if ( err ) {
                    cb( err )
                    return;
                }
                cb()
            } );
        } );
    });
}



ConnectorService.prototype.moveDir = function ( from, to, cb ) {
    var params = {
        "Bucket": bucket

    };
    listObjects(from).then(function(data){
        async.each( data.Contents, function ( item, callback ) {
            var moveObject = client.moveObject( {
                Bucket: bucket,
                Key: (to + "/" + item.Key.replace( data.Prefix, '' )),
                CopySource: (bucket + '/' + item.Key),
                'ACL': 'public-read'
            }, function ( err, data ) {
                if ( err ) {
                    return callback( err );
                }
            } );
            moveObject.on( 'copySuccess', function ( data ) {
                callback( null );
            } );
            moveObject.on( 'end', function ( data ) {
            } );
            moveObject.on( 'error', function ( data ) {
                callback( data );
            } );

        }, function (err) {
            if (err){
                cb( err )
                return;
            }
            cb()
        } );
    } );
}

ConnectorService.prototype.getFiles = function ( folder, cb ) {
    var list = client.listObjects( {
        s3Params: { "Bucket": bucket, Prefix: folder + '/' },
        recursive: false
    }, function ( err, data ) {
        if ( err ) {
            return cb( err );
        }
    } );
    list.on( 'data', function ( data ) {
        var response = [];
        _.each( data.Contents, function ( item ) {
            response.push( {
                Key: item.Key.replace( data.Prefix, '' )
            } );
        } );
        cb( null, response );
    } );
    list.on( 'end', function ( data ) {
    } );
};

ConnectorService.prototype.upload = function ( container, req, res, cb ) {
    var busboy = new Busboy( { headers: req.headers } );
    busboy.on( 'file', function ( fieldname, file, filename, encoding, mimetype ) {
        uploadS3( file, container + '/' + filename, mimetype, function ( err ) {
            if ( err ) {
                cb( err );
            } else {
                cb( null, 'ok' );
            }
        } );
    } );
    req.pipe( busboy );
};

ConnectorService.prototype.uploadFromUrl = function (container, url, callback) {
    request({
        url: url,
        encoding: null
    }, function (err, res, body) {
    if (err)
      return callback(err, res);

    client.s3.putObject({
        Bucket: bucket,
        Key: container+"/"+path.basename(res.request.path),
        ContentType: res.headers['content-type'],
        ContentLength: res.headers['content-length'],
        Body: body
    }, callback);
  })
};

ConnectorService.prototype.delete = function ( files, cb ) {
    var filesToDel = [];
    _.each( files, function ( file ) {
        filesToDel.push( {
            Key: file
        } );
    } );
    var files = client.deleteObjects( {
        Bucket: bucket,
        Delete: {
            Objects: filesToDel
        }
    } );
    files.on( 'end', function () {
        cb( null, 'ok' );
    } );
};


function uploadS3( readStream, key, mimetype, callback ) {
    var upload = Uploader.upload( {
        'Bucket': bucket,
        'Key': key,
        'ContentType': mimetype,
        'ACL': 'public-read'
    } );
    upload.on( 'error', function ( err ) {
        callback( err );
    } );
    upload.on( 'uploaded', function ( details ) {
        callback( null, details );
    } );
    readStream.pipe( upload );
};

ConnectorService.prototype.upload.shared = true;
ConnectorService.prototype.upload.accepts = [
    { arg: 'container', type: 'string' },
    { arg: 'req', type: 'object', 'http': { source: 'req' } },
    { arg: 'res', type: 'object', 'http': { source: 'res' } }
];
ConnectorService.prototype.upload.returns = { arg: 'result', type: 'object' };
ConnectorService.prototype.upload.http =
{ verb: 'post', path: '/upload/:container' };


ConnectorService.prototype.uploadFromUrl.shared = true;
ConnectorService.prototype.uploadFromUrl.accepts = [
  { arg: 'container', type: 'string' },
  { arg: 'url', type: 'string'},
];
ConnectorService.prototype.uploadFromUrl.returns = { arg: 'result', type: 'object' };
ConnectorService.prototype.uploadFromUrl.http =
  { verb: 'post', path: '/uploadFromUrl/:container' };


ConnectorService.prototype.getFiles.shared = true;
ConnectorService.prototype.getFiles.accepts = [
    { arg: 'folder', type: 'string' }
];
ConnectorService.prototype.getFiles.returns = { arg: 'files', root: true, type: 'array' };
ConnectorService.prototype.getFiles.http =
{ verb: 'get', path: '/getFiles' };

ConnectorService.prototype.delete.shared = true;
ConnectorService.prototype.delete.accepts = [
    { arg: 'files', type: 'array' }
];
ConnectorService.prototype.delete.returns = { arg: 'text', root: true };
ConnectorService.prototype.delete.http =
{ verb: 'post', path: '/delete' };


ConnectorService.prototype.moveDir.shared = true;
ConnectorService.prototype.moveDir.accepts = [
    { arg: 'from', type: 'string' },
    { arg: 'to', type: 'string' },
];
ConnectorService.prototype.moveDir.returns = { arg: 'files', root: true, type: 'array' };
ConnectorService.prototype.moveDir.http =
{ verb: 'post', path: '/moveDir' };


ConnectorService.prototype.copyDir.shared = true;
ConnectorService.prototype.copyDir.accepts = [
    { arg: 'from', type: 'string' },
    { arg: 'to', type: 'string' },
];
ConnectorService.prototype.copyDir.returns = { arg: 'files', root: true, type: 'array' };
ConnectorService.prototype.copyDir.http =
{ verb: 'post', path: '/moveDir' };