// Require the demo configuration. This contains settings for this demo, including
// the AWS credentials and target queue settings.
var config = require( "./config.json" );

// Require libraries.
var aws = require( "aws-sdk" );
aws.config.loadFromPath('./config.json');
var Q = require( 'q' );
var chalk = require( "chalk" );
var fs = require('fs');
var gm = require('gm');

// Create an instance of our SQS Client.
var sqs = new aws.SQS({
    region: config.region,
    accessKeyId: config.accessKeyId,
    secretAccessKey: config.secretAccessKey,

    // For every request in this demo, I'm going to be using the same QueueUrl; so,
    // rather than explicitly defining it on every request, I can set it here as the
    // default QueueUrl to be automatically appended to every request.
    params: {
        QueueUrl: config.queueUrl
    }
});

var s3 = new aws.S3();
var simpledb = new aws.SimpleDB();

// Proxy the appropriate SQS methods to ensure that they "unwrap" the common node.js
// error / callback pattern and return Promises. Promises are good and make it easier to
// handle sequential asynchronous data.
var receiveMessage = Q.nbind( sqs.receiveMessage, sqs );
var deleteMessage = Q.nbind( sqs.deleteMessage, sqs );


// ---------------------------------------------------------- //
// ---------------------------------------------------------- //


// When pulling messages from Amazon SQS, we can open up a long-poll which will hold open
// until a message is available, for up to 20-seconds. If no message is returned in that
// time period, the request will end "successfully", but without any Messages. At that
// time, we'll want to re-open the long-poll request to listen for more messages. To
// kick off this cycle, we can create a self-executing function that starts to invoke
// itself, recursively.
(function pollQueueForMessages() {

    console.log( chalk.yellow( "Starting long-poll operation." ) );

    // Pull a message - we're going to keep the long-polling timeout short so as to
    // keep the demo a little bit more interesting.
    receiveMessage({
        WaitTimeSeconds: 3, // Enable long-polling (3-seconds).
        VisibilityTimeout: 10
    })
    .then(
        function handleMessageResolve( data ) {

            // If there are no message, throw an error so that we can bypass the
            // subsequent resolution handler that is expecting to have a message
            // delete confirmation.
            if ( ! data.Messages ) {

                throw(
                    workflowError(
                        "EmptyQueue",
                        new Error( "There are no messages to process." )
                    )
                );

            }

            // ---
            // TODO: Actually process the message in some way :P
            // ---
            console.log( chalk.green( "Deleting:", data.Messages[ 0 ].MessageId ) );
			console.log (chalk.green(data.Messages[ 0 ].Body));
			var strParam = data.Messages[ 0 ].Body.split("/");
			var paramsToLoad = {
        
				Bucket: "lab4-weeia/"+strParam[0], //BUCKET
				Key: strParam[1]
			};
			var file = require('fs').createWriteStream('tmp/'+strParam[1]);
			var request = s3.getObject(paramsToLoad).createReadStream().pipe(file);
			request.on('finish', function (){
				console.log(chalk.green("Plik został zapisany na dysku"));
				gm('tmp/'+strParam[1])
				.implode(-1.2)
				.contrast(-6)
				//.resize(353, 257)
				.autoOrient()
				.write('tmp/changed_'+strParam[1], function (err) {
				if (err) {
					console.log(err);
				}else{
				    console.log(chalk.green('Plik zostal przetworzony'));
					//wrzucamy na s3 nowy plik
					var fileStream = require('fs').createReadStream('tmp/changed_'+strParam[1]);
					fileStream.on('open', function () {
						var paramsu = {
							Bucket: paramsToLoad.Bucket,
							Key: 'changed_'+strParam[1],
							ACL: 'public-read',
							Body: fileStream,
						};
						s3.putObject(paramsu, function(err, datau) {
    						if (err) {
    							console.log(err, err.stack);
    						}
    						else {
    							console.log(chalk.green(datau));
    							console.log(chalk.green('Upload zakonczony'));
    							var paramsdb = {
								Attributes: [
									{
                					Name:"key",
                					Value: 'changed_'+strParam[1],
                					Replace: false
									}
								],
								DomainName: "marcin.gradzki", 
								ItemName: 'Zakonczone'
    							};
    							simpledb.putAttributes(paramsdb, function(err, datass) {
    							if (err) {
    								console.log('Blad zapisu do bazy'+err, err.stack);
    							}
    							else {
    								console.log(chalk.green("Zapisano do bazy"));
    							}
    							});
    						}
						});
					});
				}
				});

			});
            // Now that we've processed the message, we need to tell SQS to delete the
            // message. Right now, the message is still in the queue, but it is marked
            // as "invisible". If we don't tell SQS to delete the message, SQS will
            // "re-queue" the message when the "VisibilityTimeout" expires such that it
            // can be handled by another receiver.
            return(
                deleteMessage({
                    ReceiptHandle: data.Messages[ 0 ].ReceiptHandle
                })
            );

        }
    )
    .then(
        function handleDeleteResolve( data ) {

            console.log( chalk.green( "Message Deleted!" ) );

        }
    )

    // Catch any error (or rejection) that took place during processing.
    .catch(
        function handleError( error ) {

            // The error could have occurred for both known (ex, business logic) and
            // unknown reasons (ex, HTTP error, AWS error). As such, we can treat these
            // errors differently based on their type (since I'm setting a custom type
            // for my business logic errors).
            switch ( error.type ) {
                case "EmptyQueue":
                    console.log( chalk.cyan( "Expected Error:", error.message ) );
                break;
                default:
                    console.log( chalk.red( "Unexpected Error:", error.message ) );
                break;
            }

        }
    )

    // When the promise chain completes, either in success of in error, let's kick the
    // long-poll operation back up and look for moar messages.
    .finally( pollQueueForMessages );

})();

// When processing the SQS message, we will use errors to help control the flow of the
// resolution and rejection. We can then use the error "type" to determine how to
// process the error object.
function workflowError( type, error ) {

    error.type = type;

    return( error );

}
