require('dotenv').config()
const express = require('express')
const bodyParser = require('body-parser')
const app = express()
// const config = { region: "us-east-1", credentials: { accessKeyId: configFile.AWS_ACCESS_KEY_ID, secretAccessKey: configFile.AWS_SECRET_ACCESS_KEY } }

const AWS = require("aws-sdk")
const path = require('path')
const dirPath = path.join(__dirname, './config.json')
const config = AWS.config.loadFromPath(dirPath);

const { S3Client, GetObjectCommand } = require("@aws-sdk/client-s3");
const client = new S3Client(config);


const { DynamoDBClient, GetItemCommand, ScanCommand } = require("@aws-sdk/client-dynamodb"); // CommonJS import

const port = process.env.PORT || 3001;
// DBs
const FINITE_DB = 'finite'
// Tables
const USER_TABLE = 'users'
const PLAYER_TABLE = 'players'
// Endpoints
const POSTS_ENDPOINT = '/api/posts'
const THREADS_ENDPOINT = '/api/threads'
const TOPICS_ENDPOINT = '/api/topics'

app.use(bodyParser.json({limit: '50mb'}));
app.use(bodyParser.urlencoded({ limit: '50mb', extended: true }))

async function downloadFileFromS3(response) {
  // let responses = []
  // let cursor = client.db(FINITE_DB).collection(USER_TABLE).find()
  // await cursor.forEach( el => {
  //   responses.push(el)
  // })
  const command = new GetObjectCommand({Bucket: 'legendary-public', Key: 'posts.json'});
  const res = await client.send(command);
  // console.log(command);
  const stream = res.Body

  const p = new Promise((resolve, reject) => {
      const chunks = []
      stream.on('data', chunk => chunks.push(chunk))
      stream.once('end', () => resolve(Buffer.concat(chunks)))
      stream.once('error', reject)
  })
  p.then(data => {
    response.send(data)
  })
}

async function getPosts(response) {
  const client = new DynamoDBClient(config);
  const command = new ScanCommand({ TableName: 'posts' });
  const res = await client.send(command);
  const items = res.Items.map((item) => {
    return {
      id: item.id.S,
      topic: item.topic.S,
      date: item.date.S,
      role: item.role.S,
      header: item.header.S,
      post: item.post.S,
      user: item.user.S,
      thread: item.thread.S,
    }
  })
  response.send(items)
}

app.get(POSTS_ENDPOINT, (req, res) => {
  console.log('GET posts ',req.body)
  getPosts(res)      
})

async function getThreads(response) {
  const client = new DynamoDBClient(config);
  const command = new ScanCommand({ TableName: 'threads' });
  const res = await client.send(command);
  console.log(res);
  const items = res.Items.map((item) => {
    return {
      id: item.id.S,
      topic: item.topic.S,
      numberThreads: Number(item.numberThreads.N),
      previewDate: item.previewDate.S,
      previewTitle: item.previewTitle.S,
      subtitle: item.subtitle.S,
      title: item.title.S
    }
  })
  response.send(items)
}

app.get(THREADS_ENDPOINT, (req, res) => {
  console.log('GET threads ',req.body)
  getThreads(res)      
})

async function getTopics(response) {
  const client = new DynamoDBClient(config);
  const command = new ScanCommand({ TableName: 'topics' });
  const res = await client.send(command);
  console.log(res);
  const items = res.Items.map((item) => {
    return {
      id: item.id.S,
      numberThreads: Number(item.numberThreads.N),
      previewDate: item.previewDate.S,
      previewTitle: item.previewTitle.S,
      subtitle: item.subtitle.S,
      title: item.title.S
    }
  })
  response.send(items)
}

app.get(TOPICS_ENDPOINT, (req, res) => {
  console.log('GET topics ',req.body)
  getTopics(res)      
})

app.put(POSTS_ENDPOINT, (req, res) => {
  console.log('PUT   ',req.body)  
  // MongoClient.connect(MONGO_URL, MONGO_OPTIONS, (err, client) => {
  //   if (err) throw err
  //   if (req.body.function === 'populate'){populate(client, req.body)}
  //   else incPlayerVolume(client, req.body)
  // })
})

async function populate(client,body) {
  await client.db(FINITE_DB).collection(body.table).insertMany(body.records)
}

async function incPlayerVolume(client,body) {
  await client.db(FINITE_DB).collection(PLAYER_TABLE).updateOne({ _id: body._id }, { $inc: { volume: body.quantity }})
}

async function signup(client,body) {
  await client.db(FINITE_DB).collection(USER_TABLE).insertOne(body)
}

app.post(POSTS_ENDPOINT, (req, res) => {
  console.log('POST  ',req.body);
  // MongoClient.connect(MONGO_URL, MONGO_OPTIONS, (err, client) => {
  //   if (err) throw err  
  //   signup(client, req.body)      
  // })
})

app.delete(POSTS_ENDPOINT, (req, res) => {
  console.log('DELETE',req.body);
  // MongoClient.connect(MONGO_URL, MONGO_OPTIONS, (err, client) => {
  //   if (err) throw err  
  //   deleteUser(client, req.body._id)      
  // })
})

app.listen(port, () => console.log(`Listening on port ${port}`))