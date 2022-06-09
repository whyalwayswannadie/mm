const MongoClient = require("mongodb").MongoClient;
const ObjectID    = require("mongodb").ObjectID;
const mm          = require('./index.js').mm
const delay       = ms => new Promise(r => setTimeout(r.bind(ms), ms))
 
;(async () => {
    const mongoClient = new MongoClient("mongodb://localhost:27017/", { useNewUrlParser: true });
    const client      = await mongoClient.connect()
    const db          = client.db('mm')
    const Savable     = mm(db).Savable
    const SlicedSavable = mm(db).sliceSavable([ObjectID("5c9571219be797377361c65a"), 'user', 'admin'])
    //const SlicedSavable = mm(db).sliceSavable([])
    //

    class Notebook extends SlicedSavable{
        static get relations(){
            return {
                owner: "notebook"
            }
        }
    }

    class User extends SlicedSavable{
        static get relations(){
            return {
                children: "parent",
                parent: ["children"],
                friends: ["friends"],
                notebook: "owner",
            }
        }
    }
    Savable.addClass(Notebook)
    Savable.addClass(User)


    let names    = ['Ivan', 'Petro', 'Mykola', 'Sashko']
    let surnames = ['Ivanopulo', 'Petrov', 'Mykolyiv', 'Alexandrov']

    let rndItem  = arr => arr[Math.floor(Math.random()*arr.length)]


    let person = new User({
        name: 'Mykola',
        surname: 'Silniy',
        phones: ['105', '1'],
        children: [
            new User({
                name: 'Marina',
                surname: 'Silnaya',
                phones: ['105', '1000503'],
            }),
            new User({
                name: 'Andrey',
                surname: 'Silniy',
                phones: ['103', '1000502'],
            }),
            new User({
                name: 'Fedor',
                surname: 'Ivanova',
                phones: ['102', '1000504'],
                notebook: new Notebook({
                    brand: 'dubovo'
                })
            })
        ]
    })

    await person.save()

    console.log(person.children.length)
    person.children[0].parent = null;
    await person.children[0].save()
    console.log(person.children.length)

    //let orphan = new User({
        //name: 'Left',
        //surname: 'Guy',
        //parent: person //check for push into and $addToSet
    //})
    //await orphan.save()

    //person.parent = orphan //check for arrize

    //await person.save()

    //console.log(orphan.children)

    //person.parent = null //check for arrize
    //await person.save()
    //person.children.pop();
    //person.children.pop();
    //console.log(person.children.length)
    //await person.save()
    



    let stamp = (new Date()).getTime()
    let prevI = 0;
    const persons = []
    //for (var i=0;i<1e10;i++){
        //let person = new User({
            //name: rndItem(names),
            //surname: rndItem(surnames),
            //phones: ['105', '1'],
            //friends: persons.slice(-(Math.random()*80))
        //})

        //await person.save(true)
        
        //persons.push(person)
        //if (persons.length > 200){
            //await (Math.random() > 0.5 ? persons.shift() : persons.pop()).save(true)
        //}
        

        //let now = (new Date()).getTime()
        //if (stamp < now - 1000){
            ////results:
            ////objects w/o relations: 2500 writes per second
            ////objects w relations: pessimistic backrelations sync, 0..80 friends of 200 latest created, ~25 per second due 0..80 saves of other friends with new one relation
            ////objects w relations: ~500 per second, save w/o backref save (but it updated in object), than, when object removed from random buffer, re-save it with updated relations
            //console.log(i, i - prevI)
            //prevI = i
            //stamp = now
        //}
    //}


    async function walker(limit=10) {
        let start = (new Date()).getTime()
        let stamp = start
        let now   = start

        let prevI = 0
        let person
        for (let uP of Savable.m.User.find({},null,{sort:['_id', -1], limit: [1]})){
            person = await uP
            console.log(person)
        }

        //let person = [...Savable.m.User.find({},null,{sort:['_id', 1], limit: [1]})][0]
        //console.log('order-huyorder', person)
        let prevPerson;
        for (var i=0;i<limit;i++){
            prevPerson = person || prevPerson
            try {
                person = person.friends ? await rndItem(person.friends) : await Savable.m.User.findOne() //walking in graph: go to random friend
            }
            catch(e){
                person = await Savable.m.User.findOne()
            }

            //console.log(prevPerson && prevPerson._id)
            //await prevPerson.delete()
            //if (persons.includes(person)){
                //console.log('WAS HERE',person._id, person.name, person.surname, person.createdAt)
            //}
            //for (let friend of person.friends){
                //await friend
            //}
            //persons.push(person)

            now = (new Date()).getTime()
            if (stamp < now - 1000){
                //results:
                //walking: 100-200 per second, not so fun...
                //loops in graph: near 0-5 on 100 steps between nodes in graph
                console.log(i, i - prevI, person._id, person.name, person.surname, person.createdAt)
                prevI = i
                stamp = now
            }
        }
        return now - start
    }

    //await walker(8531)


    //console.log(await Promise.all([walker(), walker()]))
    //console.log(await Promise.all([walker(), walker()]))



    

    //for (let child of father.children){
        //console.log(await child)
        //console.log(child.name, child.dirty)
    //}

    //let father = await Savable.m.User.findOne(ObjectID("5c9571219be797377361c65a"))
    //console.log(father);
    //(await father.children[0]).parent = null;
    //await (await father.children[0]).save();
    //console.log(father);
    






    //let notik = await Savable.m.Notebook.findOne(ObjectID('5c7c064d2ed0f4c9ab4cba4e'))

    //let SilniyeMans = await Savable.m.Savable.find({ $or: [{surname: 'Silniy'}, {surname: 'Silnaya'}]})
    //for (let manPromise of SilniyeMans){
        //let man = await manPromise;

        //console.log('man', man.name, man.surname, man.createdAt)
        //notik.owner = man
        ////notik.owner = [man]
        ////notik.owner = new Set([man])
        //break;
    //}

    //await notik.save()



    //console.log(notik)
    //notik.ram = 4;
    //notik.resolution = {width: 1920, height: 1080}
    //await notik.save()
    //console.log(await Savable.m.Notebook.findOne(ObjectID('5c7c064d2ed0f4c9ab4cba4e')))

    //while(true){
        //await (new Savable({timestamp: (new Date).getTime(), r: Math.random()})).save()
        //console.log(person)

        //await delay(1000)
    ////}

    ////let person = new Savable()
    ////person._id = ObjectID('5c7bd603ce3cbc409978203e');
    ////console.log(person)

    //let child = new Savable({
        //name: 'New One Child',
        //surname: 'Silniy',
        //phones: ['105', '1000506']
    //});

    ////console.log(await person)
    ////console.log(await person.children[1])
    //person.children.push(child)
    //child.father = person

    ////console.log(person)
    ////console.log(child)

    //await person.save()


    ////console.log(await person.children[3])
    //let p2 =new Savable({_id: ObjectID('5c7bf8f04a3a3299f7deda0d' )}, true) //check for cache hit
    //;(await new Savable({_id: ObjectID('5c7bf8f04a3a3299f7deda0d' )}, true)) //check for cache hit
    //;(await p2)
    //console.log('parent 2', p2)
    //console.log(await     p2.children[3]) //check for other hit
    //console.log(await person.children[3].father)
    //console.log(await person.children[3].father.children[1])

    ////let obj = {
        ////then(cb){
            ////process.nextTick(() => cb(obj))
        ////}
    ////}
    ////console.log(await obj)
    ////console.log('empty await', await person)//.then(p => console.log(p))
    ////console.log('sub await', (await person.children[0]))//.then(p => console.log(p))



    client.close();
})()
