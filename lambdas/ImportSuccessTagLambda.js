var AWS = require('aws-sdk');
AWS.config.update({
  region: process.env.AWS_REGION
});
const pinpoint = new AWS.Pinpoint();
exports.handler = async (event) => {
  return pinpoint.tagResource({
    ResourceArn: `arn:aws:mobiletargeting:${process.env.AWS_REGION}:${event.ExternalId}:apps/${process.env.APPLICATION_ID}/segments/${event.SegmentId}`,
    TagsModel: {
      tags: {
        'REENGAGEMENT': 'YES'
      }
    }

  }).promise();
};
