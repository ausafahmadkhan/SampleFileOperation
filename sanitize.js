const fs = require('fs');
const csv = require('csv-parser');
const _ = require('lodash')
const fastcsv = require('fast-csv');

const readStream = fs.createReadStream("./FL_insurance_sample.csv");

var sampleArray = [];
var count = 0;

readStream
        .pipe(csv())
        .on('data', (row) =>
        {
            sampleArray.push(row);
        })
        .on('error', (err) =>
        {
            console.log("readstream has been closed" + err);
        })
        .on('end', () => 
        {
            console.log("Read file successfully");
            console.log(sampleArray[0]);
            let  cleanArray = [];
            for (const sample of sampleArray) 
            {   
                if (parseInt(sample.policyID) < 130000)
                {
                    count++;
                    cleanArray.push(_.pick(sample, ['policyID', 'county', 'point_latitude', 'point_longitude','line','construction']));
                }
            }

            const writeStream = fs.createWriteStream("./clean.csv");

            console.log(`${count} entries satisfy the condition`);
            fastcsv
                    .write(cleanArray, {headers : true})
                    .pipe(writeStream)
                    .on('close', () =>
                    {
                        console.log("Successfully written");
                    });
        })



