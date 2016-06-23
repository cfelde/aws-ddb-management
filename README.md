# AWS DynamoDB table throughput management

Long ago I wrote a blog post about [building a better DynamoDB scaling tool](http://blog.cfelde.com/2014/07/building-a-better-dynamodb-throughput-scaling-tool-part-2/). So I did, but I didn't share it
with anyone. Since then, multiple people have asked me to share it, and now, finally I've given in ;)

The tool as initially released needs some work on moving various parameters into external configuration. But, as is,
it works and I've been using it for a few years now.

I don't expect to spend too much time improving it, and in fact I am myself moving off of DynamoDB. But if you want to
help out feel free to send me some pull requests.