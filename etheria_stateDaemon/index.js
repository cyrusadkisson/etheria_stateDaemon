const AWS = require('aws-sdk');
const lambda = new AWS.Lambda({ region: 'us-east-1' });
//const s3 = new AWS.S3();
//const axios = require('axios');
const dynamoDB = new AWS.DynamoDB.DocumentClient({ region: 'us-east-1' });
const pako = require('pako');

var blankMaps = {};
blankMaps["0.9"] = require('./json/v0pt9_blank.json');
blankMaps["1.0"] = require('./json/v1pt0_blank.json');
blankMaps["1.1"] = require('./json/v1pt1_blank.json');
blankMaps["1.2"] = require('./json/v1pt2_blank.json');

var v0pt9Addresses = [
	"0xe468D26721b703D224d05563cB64746A7A40E1F4",
	"0x4b1705C75fDe41E35E454DdD14E5d0a0eAC06280"
];

var v1pt0Addresses = [
	"0xe414716F017b5c1457bF98e985BCcB135DFf81F2",
	"0x629A493A94B611138d4Bee231f94f5C08aB6570A"
];

var v1pt1Addresses = [
	"0x169332Ae7D143E4B5c6baEdb2FEF77BFBdDB4011",
	"0x341Db17810769E7470b22d75127C37eec44f8179"
];

var v1pt2Addresses = [
	"0xB21f8684f23Dbb1008508B4DE91a0aaEDEbdB7E4",
	"0x111B76DBBe885D05793DE91254554F0a781D15db"
];

var openseaAddresses = [
	"0x7f268357A8c2552623316e2562D90e642bB538E5" // OpenSea Wyvern v2
];

function getRandomInt(min, max) {
	min = Math.ceil(min);
	max = Math.floor(max);
	return Math.floor(Math.random() * (max - min + 1)) + min;
}

function getHrDate() {
	return (new Date()).toISOString().replace('T', ' ').substring(0, 16) + " UTC";
}

var spreadTimer = 3000;

function getBlock(atBlock, hydrated) {
	return new Promise((resolve, reject) => {
		var sleepAmount = getRandomInt(0, spreadTimer);
		setTimeout(function() {
			web3.eth.getBlock(atBlock, hydrated).then(function(result) {
				resolve(result);
			}).catch(function(caughtError) {
				console.log("getBlock() caught error=" + caughtError);
				reject(caughtError);
			});
		}, sleepAmount);
	});
}

var Web3 = require('web3');
var web3 = new Web3(process.env.WEB3_PROVIDER_URL_1);
var lookahead = process.env.LOOKAHEAD * 1;

function arraysEqual(a, b) {
  if (a === b) return true;
  if (a == null || b == null) return false;
  if (a.length !== b.length) return false;

  // If you don't care about the order of the elements inside
  // the array, you should sort both arrays here.
  // Please note that calling sort on an array will modify that array.
  // you might want to clone your array first.

  for (var i = 0; i < a.length; ++i) {
    if (a[i] !== b[i]) return false;
  }
  return true;
}

function areStatesEqual(a, b) // uncompressed states
{
//	console.log(JSON.stringify(a));
//	console.log(JSON.stringify(b));
//	console.log("typeof a=" + typeof a);
//	console.log("typeof b=" + typeof b);
//	console.log("Array.isArray(a)=" + Array.isArray(a));
//	console.log("Array.isArray(b)=" + Array.isArray(b));

//	if (typeof a !== typeof b) {
//		console.log("a and b types differ. returning false");
//		console.log("typeof a=" + typeof a);
//		console.log("typeof b=" + typeof b);
//		return false;
//	}
//	console.log("a & b types are equal");
//
//	if (Array.isArray(a) ) {
//			console.log("a & b should be instanceof Array and are not. returning false");
//			return false;
//	}

	if (a.length !== b.length) {
		console.log("a and b lengths differ. " + a.length + " vs " + b.length + " returning false");
		console.log("a=" + a);
		console.log("b=" + b);
		return false;
	}
	console.log("a & b lengths are equal at " + a.length);

	var l = 0;
	while (l < a.length) {
		if (typeof a[l] !== typeof b[l] ) {
			console.log("a[" + l + "] and b[" + l + "] types differ. returning false");
			console.log("typeof a[" + l + "]=" + typeof a[l]);
			console.log("typeof b[" + l + "]=" + typeof b[l]);
			return false;
		}
		
		if (typeof a[l] !== "object" ) {
			console.log("should be type 'object' and isn't. returning false");
			return false;
		}

		if (Object.keys(a[l]).length !== Object.keys(b[l]).length) {
			console.log("Object.keys(a[l]) and Object.keys(b[l]) lengths differ. " + Object.keys(a[l]).length + " vs " + Object.keys(b[l]).length + " returning false");
			console.log("a=" + JSON.stringify(a));
			console.log("b=" + JSON.stringify(b));
			return false;
		}
		
		if(a[l].index !== b[l].index) {
			console.log("index changed for tile " + l + ". returning false");
			console.log("a[l].index=" + a[l].index);
			console.log("b[l].index=" + b[l].index);
			return false;
		}
		
		if(a[l].elevation !== b[l].elevation) {
			console.log("elevation changed for tile " + l + ". returning false");
			console.log("a[l].elevation=" + a[l].elevation);
			console.log("b[l].elevation=" + b[l].elevation);
			return false;
		}
		
		if(a[l].owner !== b[l].owner) {
			console.log("owner changed for tile " + l + ". returning false");
			console.log("a[l].owner=" + a[l].owner);
			console.log("b[l].owner=" + b[l].owner);
			return false;
		}
		
		if(a[l].nameRaw !== b[l].nameRaw) {
			console.log("nameRaw changed for tile " + l + ". returning false");
			console.log("a[l].nameRaw=" + a[l].nameRaw);
			console.log("b[l].nameRaw=" + b[l].nameRaw);
			return false;
		}
		
		if(a[l].name !== b[l].name) {
			console.log("name changed for tile " + l + ". returning false");
			console.log("a[l].name=" + a[l].name);
			console.log("b[l].name=" + b[l].name);
			return false;
		}
		
		if(a[l].status !== b[l].status) {
			console.log("status changed for tile " + l + ". returning false");
			console.log("a[l].status=" + a[l].status);
			console.log("b[l].status=" + b[l].status);
			return false;
		}
		
		if(a[l].blocks.length !== b[l].blocks.length) {
			console.log("blocks.length changed for tile " + l + ". returning false");
			console.log("a[l].blocks=" + JSON.stringify(a[l].blocks) + " a[l].blocks.length=" + a[l].blocks.length);
			console.log("b[l].blocks=" + JSON.stringify(b[l].blocks) + " b[l].blocks.length=" + b[l].blocks.length);
			return false;
		}
		
		if(a[l].blocks.length > 0)
		{
			var j = 0;
			while(j < a[l].blocks.length)
			{
				if(!arraysEqual(a[l].blocks[j], b[l].blocks[j]))
				{
					console.log("blocks changed for tile " + l + ", block index=" + j + ". returning false");
					console.log("a[l].blocks[j]=" + JSON.stringify(a[l].blocks[j]));
					console.log("b[l].blocks[j]=" + JSON.stringify(b[l].blocks[j]));
				}
				j++;	
			}
			console.log("a&b[" + l + "].blocks length were > 0 but all were the same. continuing.");
		}
		
		if(a[l].ownerOf !== b[l].ownerOf) {
			console.log("ownerOf changed for tile " + l + ". returning false");
			console.log("a[l].ownerOf=" + a[l].ownerOf);
			console.log("b[l].ownerOf=" + b[l].ownerOf);
			return false;
		}
		
		if(a[l].ask !== b[l].ask) {
			console.log("ask changed for tile " + l + ". returning false");
			console.log("a[l].ask=" + a[l].ask);
			console.log("b[l].ask=" + b[l].ask);
			return false;
		}
		
		l++;
	}

	return true;
}

exports.handler = async (event) => {
	console.log("event=" + JSON.stringify(event));

	return new Promise((resolve, reject) => {

		if (!event || Object.keys(event).length === 0) {
			reject(new Error("event is invalid or missing"));
			return;
		}

		if (!event.params) {
			reject(new Error("event.params is invalid or missing"));
			return;
		}

		if (!event.params.querystring) {
			reject(new Error("event.params.querystring is invalid or missing"));
			return;
		}
		console.log("querystring=" + JSON.stringify(event.params.querystring));

		if (!
			(
				event.params.querystring.version === "0.9" ||
				event.params.querystring.version === "1.0" ||
				event.params.querystring.version === "1.1" ||
				event.params.querystring.version === "1.2"
			)
		) {
			reject(new Error("Invalid or missing version parameter"));
			return;
		}
		console.log("version=" + event.params.querystring.version);

		//		if (!event.params.querystring.file || typeof event.params.querystring.file !== "string") {
		//			reject(new Error("event.params.querystring.file is invalid or missing"));
		//			return;
		//		}
		//		console.log("event.params.querystring.file=" + event.params.querystring.file);

		//		if (!event.params.querystring.atBlock)
		//			event.params.querystring.atBlock = "latest";
		//		console.log("atBlock=" + event.params.querystring.atBlock);

		var exclusiveStartKey = {
			"version": event.params.querystring.version,
			"blockNumber": 9000000000 //(event.params.querystring.atBlock * 1)
		};

		var params = {
			TableName: "EtheriaEvents2",
			KeyConditionExpression: "#vs = :vvv",
			ExpressionAttributeNames: { "#vs": "version" },
			ExpressionAttributeValues: {
				":vvv": event.params.querystring.version,
			},
			ScanIndexForward: false,
			// ProjectionExpression: projex,
			ExclusiveStartKey: exclusiveStartKey,
			Limit: 1
		};

		dynamoDB.query(params, function(err, oldData) {
			if (err) {
				console.log("Error", err);
				reject(err);
			}
			else {
				if (oldData.Items.length === 0) {
					console.log("no existing state. compressing blank map and submitting to db");
					var touchDate = new Date();
					var touchISO = touchDate.toISOString();
					var compressed = pako.deflate(JSON.stringify(blankMaps[event.params.querystring.version].tiles), { to: 'string' });
					var params = {
						TableName: 'EtheriaEvents2',
						Item: {
							'version': event.params.querystring.version,
							'blockNumber': blankMaps[event.params.querystring.version].blockNumber,
							'timestamp': blankMaps[event.params.querystring.version].timestamp,
							'state': compressed,
							'dateISO': (new Date(blankMaps[event.params.querystring.version].timestamp * 1000)).toISOString(),
							'nextBlock': blankMaps[event.params.querystring.version].blockNumber + 1,
							'touchISO': touchISO,
							'touchTimestamp': touchTimestamp
						}
					};

					dynamoDB.put(params, function(err, data) {
						if (err) {
							console.log(getHrDate() + " " + event.params.querystring.version + " " + params.TableName + " put error. ", err);
							reject();
						}
						else {
							console.log(getHrDate() + " " + event.params.querystring.version + " " + params.TableName + " put success. data=" + JSON.stringify(data));
							resolve(); // found nothing, resolve with nothing
						}
					});
				}
				else {
					console.log("existing state found with data.Items[0].blockNumber=" + oldData.Items[0].blockNumber);
					var compressedOldState = new Uint8Array(oldData.Items[0].state);
					var decompressedString = pako.inflate(compressedOldState, { to: 'string' });
					// for later comparison
					var tiles = JSON.parse(decompressedString);
					web3.eth.getBlock("latest", false).then(function(latestBlock) { // we use web3.eth.getBlock here to avoid the spreadTimer. We want it immediately. Don't need full tx info either.
						console.log("latestBlock.number=" + latestBlock.number);

						var searchToBlock = latestBlock.number;
						var b;
						if (oldData.Items[0].nextBlock > latestBlock.number) // error case (shouldn't happen). Somehow the system has recorded that it has looked past the most recent ETH block.
						{												 // In this case, search from data.Items[0].blockNumber instead (i.e. start the nextBlock count over again)
							console.log("Error case: nextBlock is beyond ETH's most recent block. Handling by resetting search to block of last DB entry + 1.");
							if (searchToBlock - oldData.Items[0].blockNumber > lookahead) {
								searchToBlock = oldData.Items[0].blockNumber + lookahead; // to avoid overload, get maximum *lookahead* blocks at a time. It'll eventually catch up
								console.log("Latest block " + latestBlock.number + " is too far ahead of the last block we checked " + oldData.Items[0].blockNumber);
								console.log("Limiting to " + lookahead + " blocks from there, ending with " + searchToBlock);
							}
							b = oldData.Items[0].blockNumber + 1; // skip the last block we checked
						}
						else { // normal case
							if (searchToBlock - oldData.Items[0].nextBlock > lookahead) {
								searchToBlock = oldData.Items[0].nextBlock + lookahead; // to avoid overload, get maximum *lookahead* blocks at a time. It'll eventually catch up
								console.log("Latest block " + latestBlock.number + " is too far ahead of the last block we checked " + oldData.Items[0].nextBlock);
								console.log("Limiting to " + lookahead + " blocks from there, ending with " + searchToBlock);
							}
							b = oldData.Items[0].nextBlock;
						}
						var getBlockPromises = [];
						while (b <= searchToBlock) {
							getBlockPromises.push(getBlock(b, true));
							b++;
						}

						Promise.all(getBlockPromises).then(function(resultArray) {
							var i = 0;
							var t = 0;
							var numberOfFirstRelevantBlock = 0;
							var targetContract = "";

							var ourAddresses = [];
							if (event.params.querystring.version === "0.9")
								ourAddresses = v0pt9Addresses;
							else if (event.params.querystring.version === "1.0")
								ourAddresses = v1pt0Addresses;
							else if (event.params.querystring.version === "1.1")
								ourAddresses = v1pt1Addresses;
							else if (event.params.querystring.version === "1.2")
								ourAddresses = v1pt2Addresses;

							var ownerOfAddressesSet = new Set();
							var a = 0;
							while (a < tiles.length) {
								ownerOfAddressesSet.add(tiles[a].ownerOf); // not lowercased;
								a++;
							}
							ownerOfAddressesSet.delete("0x0000000000000000000000000000000000000000");
							ownerOfAddressesSet.delete("");
							var ownerOfAddresses = [...ownerOfAddressesSet];
							console.log("ownerOfAddresses=" + JSON.stringify(ownerOfAddresses));

							console.log("done getting set of hydrated blocks since last. resultArray.length=" + resultArray.length);
							console.log("going to look for ourAddresses=" + JSON.stringify(ourAddresses));
							var keepLoopingResultArray = true;
							var keepLoopingTransactions = true;
							var keepLoopingOurAddresses = true;
							var blocksSearchedString = "Searched blocks ";
							while (i < resultArray.length && keepLoopingResultArray === true) {
								blocksSearchedString = blocksSearchedString + resultArray[i].number + " ";
								t = 0;
								keepLoopingTransactions = true;
								while (t < resultArray[i].transactions.length && keepLoopingTransactions === true) {
									//console.log("\tto=" + resultArray[i].transactions[t].to);
									if (ourAddresses.includes(resultArray[i].transactions[t].to)) {
										console.log("* Found transaction in this block to " + resultArray[i].transactions[t].to);
										numberOfFirstRelevantBlock = resultArray[i].number * 1; // FOUND ONE
										console.log("    numberOfFirstRelevantBlock=" + numberOfFirstRelevantBlock + " ***");
										keepLoopingResultArray = false;
										keepLoopingTransactions = false;
									}
									// atomic match hash = 0xab834bab
									else if (openseaAddresses.includes(resultArray[i].transactions[t].to) && resultArray[i].transactions[t].input.startsWith("0xab834bab")) {
										//console.log("* Found OpenSea atomic_match tx in block " + resultArray[i].number + " from " + resultArray[i].transactions[t].from + " to " + resultArray[i].transactions[t].to + " tx hash:" + resultArray[i].transactions[t].hash);
										if (resultArray[i].transactions[t].input.length < 3666) {
											console.log("    tx input data length was only " + resultArray[i].transactions[t].input.length + ". Can't get targetAddress");
										}
										else {
											targetContract = "0x" + resultArray[i].transactions[t].input.substring(3626, 3666);
											//console.log("    target contract addr:" + targetContract);
											var oA = 0;
											keepLoopingOurAddresses = true;
											while (oA < ourAddresses.length && keepLoopingOurAddresses === true) {
												if (ourAddresses[oA].toLowerCase() === targetContract) {
													console.log("FOUND OPENSEA atomic_match TX FOR OUR CONTRACT!");
													console.log("    tx:" + JSON.stringify(resultArray[i].transactions[t]));
													numberOfFirstRelevantBlock = resultArray[i].number * 1; // FOUND ONE
													console.log("    numberOfFirstRelevantBlock=" + numberOfFirstRelevantBlock + " ***");
													keepLoopingOurAddresses = false; // break this loop
													keepLoopingResultArray = false;
													keepLoopingTransactions = false;
												}
												oA++;
											}
										}
									}
									// If a 721 tile is in a multisig and being transferred to another account, the "to" is neither the wrapper contract nor OpenSea (which we check for above)
									// Instead, the "to" will be the multisig where the tx is telling the multisig to transfer the tile and only an *internal* transaction
									// hits our wrapper contract, meaning we'll miss the transfer and won't update state...
									//
									// So the logic below checks if there are any txs "to" an of our ownerOf addresses
									// AND whether or not there is a non-empty "input" property (indicating the tx is likely to a smart contract, not just an EOA)
									// when we find one of these, we do a new state pull
									//  
									// Note that it's still possible we could miss a multi-hop tx where the tile is owned by a multi-sig and the tx "to" is to some other contract 
									// that internall calls the multisig that internally calls the wrapper to initiate a transfer. And we'll miss it. But possibility is so remote, I'm not going to worry about it now.
									// 
									else if (ownerOfAddresses.includes(resultArray[i].transactions[t].to) && resultArray[i].transactions[t].input && resultArray[i].transactions[t].input.length > 0) {
										console.log("* Found tx to one of our owner addresses in block " + resultArray[i].number + " AND it had an input value, indicating a possible multi-sig tile transfer. tx hash:" + resultArray[i].transactions[t].hash);
										console.log(" tx=" + JSON.stringify(resultArray[i].transactions[t]));
										numberOfFirstRelevantBlock = resultArray[i].number * 1;
										console.log("    numberOfFirstRelevantBlock=" + numberOfFirstRelevantBlock + " ***");
										keepLoopingResultArray = false;
										keepLoopingTransactions = false;
									}
									t++;
								}
								i++;
							}
							console.log("done parsing blocks since last");
							console.log("blocksSearched=" + blocksSearchedString);
							console.log("numberOfFirstRelevantBlock=" + numberOfFirstRelevantBlock);

							var touchDate = new Date();
							var touchISO = touchDate.toISOString();
							var touchTimestamp = Math.floor(touchDate.getTime() / 1000);
							if (numberOfFirstRelevantBlock === 0) // found nothing interesting. Update row's nextBlock value
							{
								console.log("no changes detected. Setting nextBlock to searchToBlock + 1;");
								var params = {
									TableName: "EtheriaEvents2",
									Key: {
										"version": oldData.Items[0].version,
										"blockNumber": oldData.Items[0].blockNumber
									}
								};
								params.UpdateExpression = "set nextBlock = :i, touchISO = :t, touchTimestamp = :s";
								params.ExpressionAttributeValues = {
									":i": (searchToBlock + 1),
									":t": touchISO,
									":s": touchTimestamp
								};

								dynamoDB.update(params, function(err, data) {
									if (err)
										reject(new Error("[InternalServerError]: database error=" + err.message));
									else {
										resolve();
									}
								});
							}
							else	// we found a block with a relevant transaction. Do a whole state pull at numberOfFirstRelevantBlock
							{
								console.log("found tx to relevant address. Generating map state...");
								lambda.invoke({
									FunctionName: "arn:aws:lambda:us-east-1:540151370381:function:etheria_getMapState",
									//InvocationType: "Event", // force asynchronicity
									Payload: JSON.stringify({
										"body-json": {},
										"params": {
											"path": {},
											"querystring": {
												"atBlock": numberOfFirstRelevantBlock,
												"version": event.params.querystring.version
											}
										}
									}) // pass params
								}, function(err, data) {
									if (err) {
										console.log("Got error back from etheria_getMapState err=" + err);
										reject(err);
										return;
									}
									console.log("Success getting etheria_getMapState");
									var newMapEnvelope = JSON.parse(data.Payload);
									// when we get the state back...
									// 1. update the cached map in S3  (we always want to do this to update the timestamp, even if no map changes)
									// 2. compare the previous map to the new one
									// 3. process any builds
									if (newMapEnvelope.tiles) { // a check to make sure what we're getting back has a valid map
										console.log("newMapEnvelope had tiles");
										var compressed = pako.deflate(JSON.stringify(newMapEnvelope.tiles), { to: 'string' });
										//										console.log("JSON.stringify(compressed)=" + JSON.stringify(compressed));
										//										console.log("JSON.stringify(compressedOldState)=" + JSON.stringify(compressedOldState));
										//										console.log("typeof compressed=" + typeof compressed);
										//										console.log("typeof compressedOldState=" + typeof compressedOldState);
										//										console.log("compressed.length=" + compressed.length);
										//										console.log("compressedOldState.length=" + compressedOldState.length);
//										function areEqual(a, b) {
//											var akeys = Object.keys(a);
//											var bkeys = Object.keys(b);
//											if (akeys.length !== bkeys.length) {
//												console.log("akeys and bkeys lengths differ. " + akeys.length + " vs " + bkeys.length + " returning false");
//												console.log("akeys=" + JSON.stringify(akeys));
//												console.log("bkeys=" + JSON.stringify(bkeys));
//												return false;
//											}
//											var k = 0;
//											while (k < akeys.length) {
//												if (a[akeys[k]] !== b[bkeys[k]]) {
//													console.log("found differing element at k=" + k + ". a[akeys[k]]=" + a[akeys[k]] + " and b[bkeys[k]]=" + b[bkeys[k]]);
//													return false;
//												}
//												k++;
//											}
//											return true;
//										}

										if (areStatesEqual(tiles, newMapEnvelope.tiles)) // nothing changed. state is the same. Update existing row and keep searching
										{
											console.log("tiles and newMapEnvelope.tiles were equal. updating existing row");
											var params = {
												TableName: "EtheriaEvents2",
												Key: {
													"version": oldData.Items[0].version,
													"blockNumber": oldData.Items[0].blockNumber
												}
											};
											params.UpdateExpression = "set nextBlock = :i, touchISO = :t, touchTimestamp = :s";
											params.ExpressionAttributeValues = {
												":i": (numberOfFirstRelevantBlock + 1),
												":t": touchISO,
												":s": touchTimestamp
											};

											dynamoDB.update(params, function(err, data) {
												if (err)
													reject(new Error("[InternalServerError]: database error=" + err.message));
												else {
													resolve();
												}
											});
										}
										else { // state changed. create entirely new row
											console.log("tiles and newMapEnvelope.tiles were NOT equal. creating new row");
											var params = {
												TableName: 'EtheriaEvents2',
												Item: {
													'version': event.params.querystring.version,
													'blockNumber': numberOfFirstRelevantBlock,
													'timestamp': newMapEnvelope.timestamp,
													'state': compressed,
													'dateISO': (new Date(newMapEnvelope.timestamp * 1000)).toISOString(),
													'nextBlock': numberOfFirstRelevantBlock + 1,
													'touchISO': touchISO,
													'touchTimestamp': touchTimestamp
												}
											};

											dynamoDB.put(params, function(err, data) {
												if (err) {
													console.log(getHrDate() + " " + event.params.querystring.version + " " + params.TableName + " put error. ", err);
													reject();
												}
												else {
													console.log(getHrDate() + " " + event.params.querystring.version + " " + params.TableName + " put success. data=" + JSON.stringify(data));
													resolve(); // found nothing, resolve with nothing
												}
											});
										}
									}
									else {
										// something was wrong with the new map
										console.log("something was wrong with the new map data=" + JSON.stringify(data));
										resolve();
									}
								}); // lambda invoke
							}
						}).catch(function(caughtError) {
							console.log("Promise.all(getBlockPromises) caught error=" + caughtError);
							reject(caughtError);
						}); // get latest block
					}).catch(function(caughtError) {
						console.log("web3.eth.getBlock() caught error=" + caughtError);
						reject(caughtError);
					}); // get latest block
				}
			}
		});
	}); // end return Promise
};

//exports.handler(
//	{
//		"body-json": {},
//		"params": {
//			"path": {},
//			"querystring": {
//				"version": "1.2",
//				"atBlock": "latest"
//			}
//		}
//	}
//);
