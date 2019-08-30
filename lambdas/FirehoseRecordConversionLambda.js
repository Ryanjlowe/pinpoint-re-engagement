exports.handler = async (event) => {
    const output = event.records.map((record) => {
        const data = JSON.parse(new Buffer(record.data, 'base64').toString('ascii'));
        return {
            data: new Buffer.from(JSON.stringify(data) + '\n').toString('base64'),
            recordId: record.recordId,
            result: 'Ok'
        };
    });
    return {records: output};
};
