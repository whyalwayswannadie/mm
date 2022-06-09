const { MongoClient, ObjectID }  = require("mongodb");
const {asynchronize, openPromise } = require('./asynchronize')

const mm = db => {
    class Savable {
        constructor(obj, ref, empty=false){
            this._id    = null
            this._ref   = ref
            this._class = this.__proto__.constructor.name
            this._empty = true

            Savable.addClass(this.__proto__.constructor)

            if (obj){
                this.populate(obj)
                this._empty = empty
            }
        }

        backupRelations(){
            this._loadRelations = {};
            for (const relation in this.__proto__.constructor.relations){
                this._loadRelations[relation] = this[relation] instanceof Array ? [...this[relation]] : this[relation]
            }
        }



        populate(obj){
            const convertSavables = (obj) => {
                for (const key in obj){
                    if (Savable.isSavable(obj[key])){
                        obj[key] = (this._ref && 
                                    Savable.equals(obj[key], this._ref)) ? 
                                                       this._ref : 
                                                       Savable.newSavable(obj[key], this)
                    }
                    else if (typeof obj[key] === 'object'){
                        convertSavables(obj[key])
                    }
                }
            }

            Object.assign(this, obj)



            convertSavables(this)

            this.backupRelations()
            //this._id = obj._id
        }

        get _empty(){
            return !!this.then
        }

        set _empty(value){
            if (value){
                //TODO: list of callbacks, because then can be called many times, and
                //it's not reason to repeat query to db
                this.then = (cb, err) => {

                    if (!this._id)    err(new ReferenceError('Id is empty'))
                    if (!this._class) err(new ReferenceError('Class is empty'))

                    const promise = openPromise()

                    this.collection.findOne(this._id).then( data => {
                        if (!data){
                            let error = new ReferenceError('Document Not Found')
                            if (typeof err === 'function') err(error)
                            else promise.reject(error)
                        }
                        else {
                            delete this.then
                            this.populate(data)
                            if (typeof cb === 'function')
                                promise.resolve(cb(this))
                            else {
                                promise.resolve(this)
                            }
                        }
                    })
                    return promise
                }
            }
            else {
                delete this.then
            }
        }

        get createdAt(){
            return this._id ? new Date(this._id.getTimestamp()) : null
        }

        get collection(){
            return db.collection(this._class)
        }

        async save(noSync=false){
            if (this.validate && !(await this.validate())){
                throw new SyntaxError(`validation error on entity ${this._id} of class ${this.constructor.name} (${this.name || this.key}) save`)
            }
            if (this.empty) return this;

            const syncRelations = async () => {
                if (noSync) return
                if (!(this && this.__proto__ && this.__proto__.constructor && this.__proto__.constructor.relations)) return 


                async function getValueByField(field, savable) {
                    let path = field.split('.');
                    await savable//.catch(e => console.log('GET VALUE BY FIELD ERROR'));
                    let result = savable;
                    let prev;
                    let lastKey = path.pop()
                    while (prev = result, result = result[path.shift()] && path.length);
                    return {value: prev[lastKey], obj: prev, lastKey};
                }

                for (const relation in this.__proto__.constructor.relations){
                    let backRef = this.__proto__.constructor.relations[relation]
                    if (Array.isArray(backRef)) backRef = backRef[0]

                    const loadRelation = this._loadRelations[relation]
                    const loadRelationAsArray = loadRelation instanceof Savable ? [loadRelation] : loadRelation

                    let {value, obj, lastKey: key} = await getValueByField(relation, this)
                    const valueAsArray = value instanceof Savable ? [value] : value
                    if (loadRelationAsArray){ //check for removed Refs
                        const removedRefs = valueAsArray ? 
                                loadRelationAsArray.filter(ref => !Savable.existsInArray(valueAsArray, ref)) : 
                                loadRelationAsArray
                        for (const ref of removedRefs){
                            try { await ref } catch (e) {console.log('SYNC RELATIONS REMOVE ERROR') }

                            await ref.removeRelation(this, relation)
                        }
                    }
                    if (valueAsArray){ //check for added refs
                        for (const foreignSavable of valueAsArray) {
                            try { await foreignSavable } catch (e) {console.log('SYNC RELATIONS ADD ERROR') }

                            let foreignLoadRelationsAsArray = Savable.arrize(foreignSavable._loadRelations[backRef])
                            if (foreignSavable && !Savable.existsInArray(foreignLoadRelationsAsArray, this)){
                                await foreignSavable.setRelation(this, relation)
                            }
                        }
                    }
                }
            }

            async function recursiveSlicer(obj){
                let result = obj instanceof Array ? [] : {}
                for (const key in obj){

                    if (obj[key] && typeof obj[key] === 'object'){
                        if (obj[key] instanceof Savable){
                            if (!(obj[key]._id)){
                                await obj[key].save().catch(err => console.log('ERR', err))
                            }
                            result[key] = obj[key].shortData()
                        }
                        else {
                            result[key] = await recursiveSlicer(obj[key])
                        }
                    }
                    else {
                        result[key] = obj[key]
                    }
                }
                return result;
            }

            const {_id, _empty, _ref, _loadRelations, then, ...toSave} = await recursiveSlicer(this)

            //TODO: UPSERT
            if (!this._id){ //first time
                const { insertedId } = await this.collection.insertOne(toSave)
                this._id = insertedId
            }
            else { //update
                await this.collection.updateOne({_id: this._id},  {$set: toSave}).catch(err => console.log('UPDATE ERR', err))
            }

            await syncRelations()
            this.backupRelations()
            return this
        }

        // method to get ref snapshot (empty object with some data). gives optimizations in related objects
        // place for permission, owner, probably short relations 
        shortData(){
            const {_id, _class} = this
            return { _id, _class }
        }


        async setRelation(ref, refRelationName){
            await this
            const ourRelation = ref.__proto__.constructor.relations[refRelationName]
            const ourArray    = ourRelation instanceof Array 
            const ourRelationName = ourArray ? ourRelation[0] : ourRelation

            let shortQuery = {[ourRelationName]: ref.shortData()}
            let query;

            if (ourArray || this[ourRelationName] instanceof Array){
                this[ourRelationName] = this[ourRelationName] || []
                this[ourRelationName] = this[ourRelationName] instanceof Array ? this[ourRelationName] : [this[ourRelationName]]

                if (!Savable.existsInArray(this[ourRelationName], ref)) {
                    this[ourRelationName].push(ref)
                    if (this._id && this._loadRelations[ourRelationName] instanceof Array){
                        this._loadRelations[ourRelationName].push(ref)
                    }
                }

                query = {$addToSet: shortQuery}
            }
            else {
                this[ourRelationName] =  ref
                this._id && (this._loadRelations[ourRelationName] = ref)
                query = {$set: shortQuery}
            }

            console.log('SET RELATION:', query)

            if (this._id){
                console.log('SET RELATION:', query)
                await this.collection.updateOne({_id: this._id},  query).catch(err => console.log('UPDATE ERR', err))
            }
        }

        async removeRelation(ref, refRelationName){ //i. e. this = child, ref = parent object, refRelationName = children in parent
            await this
            const ourRelation = ref.__proto__.constructor.relations[refRelationName]
            const ourArray    = ourRelation instanceof Array 
            const ourRelationName = ourArray ? ourRelation[0] : ourRelation


            if (this._id){
                const query = ourArray ? {$pull: {[ourRelationName]: ref.shortData()}}
                                       : {$set:  {[ourRelationName]: null}}
                console.log('REMOVE RELATION:', query)
                await this.collection.updateOne({_id: this._id},  query).catch(err => console.log('UPDATE ERR', err))
            }

            (this[ourRelationName] instanceof Array) ? this._loadRelations[ourRelationName] = this[ourRelationName] = this[ourRelationName].filter(ourRef => !ourRef.equals(ref))
                                                     : this[ourRelationName] = null;
        }

        async delete(noRefs=false){
            if (!noRefs) for (const relation in this.__proto__.constructor.relations){
                const backRef = this.__proto__.constructor.relations[relation]

                const loadRelation = this._loadRelations && this._loadRelations[relation]
                const loadRelationAsArray = loadRelation instanceof Savable ? [loadRelation] : loadRelation

                if (loadRelationAsArray){
                    for (const ref of loadRelationAsArray){
                        try {
                            await ref
                        }
                        catch (e) {console.log('DELETE SYNC RELATIONS ERROR') }
                        await ref.removeRelation(this, relation)
                    }
                }
            }
            const id  = this._id
            const col = this._class && this.collection

            for (let key in this)
                delete this[key]

            delete this.__proto__

            if (col)
                return await col.deleteOne({_id: id})
        }



        static arrize(value){
            if (Array.isArray(value)) return value
            if (value) return [value]
            return []
        }

        static equals(obj1, obj2){
            if (!obj1 || !obj2) return false
            if(!obj1._id) return obj1 === obj2
            if(!obj2._id) return obj1 === obj2
            return obj1._id.toString() === obj2._id.toString()
        }

        equals(obj){
            return Savable.equals(this, obj)
        }

        static existsInArray(arr, obj){
            if (!Array.isArray(arr)) return false

            let filtered = arr.filter(item => Savable.equals(item, obj))
            return filtered.length
        }

        static isSavable(obj){
            return obj && obj._id && obj._class
        }

        static newSavable(obj, ref, empty=true){
            let className = obj._class || "Savable"
            className     = Savable.classes[className] ? className : "Savable"
            if (obj.__proto__.constructor === Savable.classes[className]){
                return obj
            }
            
            return new Savable.classes[className](obj, ref, empty)
        }

        static addClass(_class){ //explicit method to add class to Savable registry for instantiate right class later
            (typeof _class == 'function') && (Savable.classes[_class.name] = _class)
        }


        static get m(){ 
            return Savable._m = (Savable._m || (new Proxy({}, {
                get(obj, _class){
                    if (_class in obj){
                        return obj[_class]
                    }

                    const applyCursorCalls = (cursor, calls) =>{
                        if (!calls) return cursor;
                        for (let [method, params] of Object.entries(calls)){
                            if (typeof cursor[method] !== "function"){
                                throw new SyntaxError(`Wrong cursor method ${method}`)
                            }
                            cursor = cursor[method](...params)
                        }
                        return cursor;
                    }

                    return  obj[_class] = {
                        * find(query, cursorCalls={}){
                            //console.log(query)
                            let cursor = applyCursorCalls(db.collection(_class).find(query), cursorCalls)
                            let cursorGen = asynchronize({s: cursor.stream(), 
                                                          chunkEventName: 'data', 
                                                          endEventName: 'close',
                                                          errEventName: 'error',
                                                          countMethodName: 'count'})

                            for (const pObj of cursorGen()){
                                yield new Promise((ok, fail) => 
                                    pObj.then(obj => (/*console.log(obj),*/ok(Savable.newSavable(obj, null, false))), 
                                              err => fail(err)))
                            }
                        },
                        async count(query, cursorCalls={}){
                            let cursor = applyCursorCalls(db.collection(_class).find(query), cursorCalls)
                            return await cursor.count(true)
                        },
                        async findOne(query){
                            let result = await db.collection(_class).findOne(query)
                            if (result)
                                return Savable.newSavable(result, null, false)
                            return result
                        }
                    }
                },

                set(obj, propName, value){
                }
            })))
        }

        static get relations(){ 
            //empty default relations, acceptable: {field: foreignField}, where:
            //field and foreign field can be Savable, Array or Set
            //both fields can be specified as "field", "field.subfield" 
            //or field: {subfield: foreignField} //TODO later if needed
            //TODO: move it into object instead of class to give more flexibility, for example
            //if person has children, it can have backRef father or mother depending on sex:
            //return {
            //    children: this.sex === 'male' ? 'father': 'mother'
            //}
            //
            //return {
            //  parent: ["children"],
            //  notebooks: "owner"
            //}
            return {}
        }
    }

    Savable.classes                                  = {Savable}

    /**
     * sliceSavable - slice (limit) Savables for some permission
     * Array userACL - array of objectIDs, words or savable refs - current user, group objectid, or `tags` or `role` (ACL)
     */

    function sliceSavable(userACL){
        userACL = userACL.map(tag => tag.toString())
        //console.log(userACL)
        class SlicedSavable extends Savable {
            constructor(...params){
                super  (...params)

                if (!this._empty){
                    this.___permissionsPrepare()
                }
            }

            ___permissionsPrepare(){
                if (this._empty)          return
                if (!this.___permissions) this.___permissions = {}

                for (let [perm, acl] of Object.entries(this.__proto__.constructor.defaultPermissions)){
                    if (!this.___permissions[perm]){
                        this.___permissions[perm] = [...acl]
                    }
                }
            }

            ___permissionCan(permission, permissions=this.___permissions, obj=this){
                const acl = (permissions && 
                                permissions[permission] || 
                                    this.__proto__.constructor.defaultPermissions[permission]).map(tag => tag.toString())
                if (!this._id && permission === 'read') return true; //if new entity you can anything
                if (acl.includes('owner') && obj.___owner && userACL.includes(obj.___owner.toString())){
                    return true
                }
                for (let uTag of userACL){
                    if (acl.includes(uTag)){
                        return true
                    }
                }
                return false
            }

            populate(obj){ //place to check read permission
                //console.log(obj)
                if (!this.___permissionCan('read', obj.___permissions, obj)){
                    throw new ReferenceError(`No Access To Entity ${this._id} of class ${this._class} for acl ${userACL}`)
                }
                super.populate(obj)
            }


            async save(noSync=false){
                if (!this._id && !this.___permissionCan('create'))
                    throw new ReferenceError(`Permission denied Create Entity of class ${this._class}`)
                if (this._id && !this.___permissionCan('write'))
                    throw new ReferenceError(`Permission denied Save Entity ${this._id} of class ${this._class}`)

                if (!this._id){
                    this.___owner = userACL[0] //TODO fix objectid troubles 
                    //console.log(typeof this.___owner, this.___owner)
                }
                return await super.save(noSync)
            }

            async setRelation(ref, refRelationName){
                await this
                const ourRelation = ref.__proto__.constructor.relations[refRelationName]
                const ourArray    = ourRelation instanceof Array 
                const ourRelationName = ourArray ? ourRelation[0] : ourRelation

                if (!this._id || this.___permissionCan('write') || 
                    (this.__proto__.constructor.guestRelations.includes(ourRelationName) && this.___permissionCan('read')))
                        return await super.setRelation(ref, refRelationName)

                throw new ReferenceError(`Permission denied Set Relation Entity ${this._id} of class ${this._class} ref: ${ref._id} of class ${ref._class}`)
            }

            async removeRelation(ref, refRelationName){
                await this;
                const ourRelation = ref.__proto__.constructor.relations[refRelationName]
                const ourArray    = ourRelation instanceof Array 
                const ourRelationName = ourArray ? ourRelation[0] : ourRelation

                if (!this._id || this.___permissionCan('write') || 
                    (this.__proto__.constructor.guestRelations.includes(ourRelationName) && this.___permissionCan('read')))
                        return await super.removeRelation(ref, refRelationName)

                throw new ReferenceError(`Permission denied Remove Relation Entity ${this._id} of class ${this._class} ref: ${ref._id} of class ${ref._class}`)
            }


            async delete(noRefs=false){
                if (!this.___permissionCan('delete'))
                    throw new ReferenceError(`Permission denied Delete Entity ${this._id} of class ${this._class}`)
                return await super.delete(noRefs)
            }

            static ___permissionQuery(permission){
                //const withObjectIDs = userACL.map((a,id) => (id = new ObjectID(a)) && id.toString() === a ? id : a)
                const withObjectIDs = userACL
                return {
                    $or: [
                          {[`___permissions.${permission}`]: {$in: withObjectIDs}},
                          {$and: [{[`___permissions.${permission}`]: "owner"},
                                             {___owner: userACL[0]}]}]
                    }
                }

            static get m() {
                return SlicedSavable._sm = (SlicedSavable._sm || (new Proxy({}, {
                        get(obj, _class){
                                if (_class in obj){
                                        return obj[_class]
                                }

                                return  obj[_class] = {
                                    * find(query,  cursorCalls={}){
                                        const originalClass = Savable.classes[_class]
                                        Savable.addClass(SlicedSavable.classes[_class])
                                        let permittedQuery = {$and: [SlicedSavable.___permissionQuery('read') ,query]}
                                        //console.log(JSON.stringify(permittedQuery, null, 4))
                                        let iter = Savable.m[_class].find(permittedQuery, cursorCalls)
                                        Savable.addClass(originalClass)
                                        yield* iter;
                                    },
                                    async count(query, cursorCalls={}){
                                        let permittedQuery = {$and: [SlicedSavable.___permissionQuery('read') ,query]}
                                        return await Savable.m[_class].count(permittedQuery, cursorCalls)
                                    },
                                    async findOne(query){
                                        const originalClass = Savable.classes[_class]
                                        Savable.addClass(SlicedSavable.classes[_class])
                                            
                                        const permittedQuery = {$and: [SlicedSavable.___permissionQuery('read') ,query]}
                                        const p = Savable.m[_class].findOne(permittedQuery)
                                        Savable.addClass(originalClass)
                                        
                                        return await p;
                                    }
                                }
                        },

                        set(obj, propName, value){
                        }
                })))
            }

            static get defaultPermissions(){
                return {
                    //savable refs, objectid's, words like 'tags' or 'roles'
                    read: ['owner', 'user'],
                    write: ['owner', 'admin'],
                    create: ['user'],
                    delete: ['admin'],

                    /*permission
                     * TODO: permissions for read and write permissions
                     *
                     */
                }
            }

            static get guestRelations(){ //guest relations are accessible to write by setRelation or removeRelation even if no write permission, only with read
                return []
            }
        }

        return SlicedSavable
    }


    return {Savable, sliceSavable}
}

async function connect(dbName, dsn=("mongodb://" + process.env.user + ":" + process.env.password + "@mongo:27017/")){
    if (!dbName)
        throw new ReferenceError(`db name does not provided`)

    const mongoClient = new MongoClient(dsn, { useNewUrlParser: true });
    const client      = await mongoClient.connect()
    const db          = client.db(dbName)
    const {Savable, sliceSavable: slice}     = mm(db)

    return {
        Savable, 
        slice,
    }
}

module.exports = {
    mm,
    connect
}
