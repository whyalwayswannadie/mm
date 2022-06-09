const MongoClient = require("mongodb").MongoClient;
const ObjectID    = require("mongodb").ObjectID;
const mm          = require('./mm.js')
const delay       = ms => new Promise(r => setTimeout(r.bind(ms), ms))
 
;(async () => {
    const mongoClient = new MongoClient("mongodb://localhost:27017/", { useNewUrlParser: true });
    const client      = await mongoClient.connect()
    const db          = client.db('mm')
    const Savable     = mm(db).Savable
    //const SlicedSavable = mm(db).sliceSavable([])
    //


    class User extends Savable{
        static get relations(){
            return {
                children: "parent",
                parent: "children",
                friends: "friends",
            }
        }
    }

    Savable.addClass(User)

	//upsert...
    let admin = (await Savable.m.User.findOne({login: 'admin'})) || 
		 (await (new User({login: 'admin'})).save())
    console.log(admin)
    let looser = (await Savable.m.User.findOne({login: 'looser'})) || 
		 (await (new User({login: 'looser'})).save())
    console.log(looser)
    const SlicedSavable = mm(db).sliceSavable([admin._id, ])
	
    class Notebook extends SlicedSavable{
	    //nothing at all
    }
    SlicedSavable.addClass(Notebook)
	
    //let notebook = new Notebook({
//	    brand: 'dell'
//	})
 //   await notebook.save()
  //  let notebook2 = new Notebook({
//	    brand: 'hp'
//	})
 //   await notebook2.save()

//	console.log(notebook)
//	console.log(notebook2)
	console.log('findone', await SlicedSavable.m.Notebook.findOne({brand: 'dubovo'}))
	for (const notik of SlicedSavable.m.Notebook.find({})){
		let n = await notik
            //console.log(n)
		//await n.delete() 
		//n.changed = true;
//		await n.save()
	}
})()
 
