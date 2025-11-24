require('dotenv').config();
const express = require('express');
const { google } = require('googleapis');
const { BlobServiceClient } = require('@azure/storage-blob');
const app = express();
app.use(express.static('public'));

const SA_BASE64 = process.env.GOOGLE_SERVICE_ACCOUNT_JSON_BASE64;
const DRIVE_FOLDER_ID = process.env.GOOGLE_DRIVE_FOLDER_ID;
const CONTAINER_NAME = process.env.CONTAINER_NAME;
const SAS_URL = process.env.AZURE_BLOB_SAS_URL;

if (!SA_BASE64 || !DRIVE_FOLDER_ID || !CONTAINER_NAME || !SAS_URL) {
    throw new Error("Faltando alguma variável de ambiente obrigatória");
}

// Google Drive Client
function driveClient() {
    const json = JSON.parse(Buffer.from(SA_BASE64, "base64").toString("utf8"));
    const auth = new google.auth.JWT(
        json.client_email,
        null,
        json.private_key,
        ["https://www.googleapis.com/auth/drive.readonly"]
    );
    return google.drive({ version: "v3", auth });
}

function blobClient() {
    return new BlobServiceClient(SAS_URL);
}


async function createContainerIfNotExists() {
    const client = blobClient();
    const container = client.getContainerClient(CONTAINER_NAME);
    try {
        await container.createIfNotExists();
        console.log("Container criado ou já existia:", CONTAINER_NAME);
    } catch (err) {
        console.error("Erro ao criar container:", err.message);
    }
}
createContainerIfNotExists();


async function listDriveFiles() {
    const drive = driveClient();
    const res = await drive.files.list({
        q: `'${DRIVE_FOLDER_ID}' in parents and trashed=false`,
        fields: "files(id,name)"
    });
    return res.data.files;
}

async function listBlobFiles() {
    const client = blobClient();
    const container = client.getContainerClient(CONTAINER_NAME);
    const arr = [];
    for await (let item of container.listBlobsFlat()) {
        arr.push({ name: item.name });
    }
    return arr;
}

async function downloadDriveFile(id) {
    const drive = driveClient();
    const res = await drive.files.get({ fileId: id, alt: "media" }, { responseType: "stream" });
    return res.data;
}

async function uploadToBlob(fileName, stream) {
    const client = blobClient();
    const container = client.getContainerClient(CONTAINER_NAME);
    const blob = container.getBlockBlobClient(fileName);
    await blob.uploadStream(stream);
}

// Rotas
app.get("/list-source", async (req, res) => {
    res.json(await listDriveFiles());
});

app.get("/list-dest", async (req, res) => {
    res.json(await listBlobFiles());
});

app.post("/migrate", async (req, res) => {
    const files = await listDriveFiles();
    const results = [];
    for (const file of files) {
        try {
            const stream = await downloadDriveFile(file.id);
            await uploadToBlob(file.name, stream);
            results.push({ name: file.name, status: "success" });
        } catch (err) {
            results.push({ name: file.name, status: "error", error: err.message });
        }
    }
    res.json(results);
});

app.listen(process.env.PORT || 3000, () => console.log("Servidor rodando na porta", process.env.PORT || 3000));
