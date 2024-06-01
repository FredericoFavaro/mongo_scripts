const mongoose = require('mongoose');

// Conexão com o MongoDB
const uriA ='mongodb://127.0.0.1:27017/discord_bot_hml';
const uriB ='mongodb://127.0.0.1:27017/fap2024';

// Definindo os esquemas
const schema = new mongoose.Schema({
  nome: {
    type: String,
    required: true,
  },
  cpf: {
    type: String,
    required: true,
  },
  email: {
    type: String,
    required: false,
  },
  cargos: {
    type: Array,
    required: true,
  }
});

const connectionA = mongoose.createConnection(uriA);
const connectionB = mongoose.createConnection(uriB);

const ModelA = connectionA.model('users', schema);
const ModelB = connectionB.model('alunos', schema);

async function testRawConnection() {
  try {
    const rawDbB = connectionB.db;
    const collections = await rawDbB.collections();
    console.log('Collections in DB:', collections.map(col => col.collectionName));
    
    const rawCollectionB = rawDbB.collection('alunos');
    const rawDocumentsB = await rawCollectionB.find().toArray();
    console.log(`Raw Test: Found ${rawDocumentsB.length} documents in alunos`);
    //console.log('Raw Documents:', rawDocumentsB);
  } catch (error) {
    console.error('Raw Test: Error fetching documents from alunos:', error);
  }
}

async function mergeCollections() {
  try {
    const documentsB = await ModelB.find();
    console.log(`Found ${documentsB.length} documents in alunos`);

    for (const docB of documentsB) {
      const docA = await ModelA.findOne({ cpf: docB.cpf });

      if (docA) {
        // Atualizar documento existente na coleção A
        let updated = false;

        // Verifica e atualiza campos simples
        ['nome', 'email', 'cpf'].forEach(field => {
          if (docA[field] !== docB[field]) {
            docA[field] = docB[field];
            updated = true;
          }
        });
        console.log(docB.cargos)
       
        // Atualiza o array de cargos
        if (Array.isArray(docB.cargos) && docB.cargos.length > 0) {
          for (const cargoB of docB.cargos) {
            const exists = docA.cargos.some(cargoA => {
              return JSON.stringify(cargoA) === JSON.stringify(cargoB);
            });

            if (!exists) {
              docA.cargos.push(cargoB);
              updated = true;
            }
          }
        } else {
          console.warn(`Cargos in docB with cpf ${docB.cpf} is empty or not an array`);
        }

        if (updated) {
          await docA.save();
          console.log(`Updated document with cpf ${docA.cpf}`);
        }
      } else {
        // Adicionar novo documento na coleção A
        await ModelA.create({
          nome: docB.nome,
          cpf: docB.cpf,
          email: docB.email,
          cargos: docB.cargos
        });
        console.log(`Created new document with cpf ${docB.cpf}`);
      }
    }

    console.log('Merge completed successfully.');
  } catch (error) {
    console.error('Error merging collections:', error);
  } finally {
    // Desconecta de ambas as conexões
    await connectionA.close();
    await connectionB.close();
    console.log('Connections closed');
  }
}

connectionB.once('open', async () => {
  await testRawConnection();
  await mergeCollections();
});

