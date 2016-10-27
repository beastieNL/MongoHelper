<?php

namespace MongoHelper;

class MongoHelper
{

    public function __construct($username, $password, $database, $hostname = '127.0.0.1', $port = 27018)
    {
        #$mongo = new \MongoDB\Client("mongodb://".$username.":".$password."@127.0.0.1/".$database, 
        #    [], 
        #    ['typeMap' => ['root' => 'array', 'document' => 'array', 'array' => 'array']]);

        $mongo = new \MongoDB\Client("mongodb://".$username.":".$password."@127.0.0.1/");
        $this->db = $mongo->$database;
    }

    public function insert($collection, $data)
    {
        $retval = $this->db->$collection->insertOne($data);

        return (string)$retval->getInsertedId();
    }

    public function findOne($collection, $identifier, $convertToArray=true, $removeKey=false)
    {
        if (is_array($identifier)) {
            $retVal = $this->db->$collection->findOne($identifier);
        } else {
            $retVal = $this->db->$collection->findOne(array('_id'=>new \MongoDB\BSON\ObjectID($identifier)));
        }

        if ($convertToArray === true) {
            if ($removeKey === true) {
                return (array)$retVal;
            } else {
                return array((array)$retVal);
            }
        } else {
            return (array)$retVal;
        }
    }

    public function findAll($collection, $convertToArray=true, $filter = array())
    {
        if ($convertToArray === true) {
            $result = $this->db->$collection->find($filter);

            $return = array();
            foreach ($result as $entry) {
                $id = (string)$entry['_id'];
                $return[$id] = (array)$entry;
            }
            return $return;
        } else {
            return $this->db->$collection->find($filter);
        }
    }

    public function remove($collection, $id)
    {
        return $this->db->$collection->deleteOne(array('_id' => new \MongoDB\BSON\ObjectID($id)));
    }

    public function removeAll($collection, $criteria=array())
    {
        return $this->db->$collection->deleteMany($criteria);
    }

    public function update($collection, $identifier, $data, $upsert = false)
    {
        if (isset($data['_id'])) {
            unset($data['_id']);
        }
        $upsertCriteria = ($upsert) ? array("upsert" => true) : array();

        $criteria = (is_array($identifier)) ? $identifier : array('_id'=>new \MongoDB\BSON\ObjectID($identifier));

        $this->db->$collection->updateOne($criteria ,array('$set'=>$data),$upsertCriteria);
    }

    public function storeFile($collection, $file, $fileData)
    {
        if (!is_file($file)) { # Raw data, not a file, store it to temp file (needed to store in Mongo)
            $tmpFile = tempnam("/tmp", "PHPMongoTmpFile");
            if (!file_put_contents($tmpFile,$file)) {
                throw new \Exception("Can't write to tmpfile for Mongo data");
            }
            $file = $tmpFile;
        }

        # Insert meta and file info in files collection
        $var = $collection.'.files';
        $result = $this->db->$var->insertOne(array('metadata' =>$fileData));
        $insertedId = $result->getInsertedId();

        /* Insert file data in chunks collection, only insert one entry, chunking is not yet supported
         *
         * use 0 as 'n', a simple index to match all chunks
         */
        $var = $collection.'.chunks';
        if (filesize($tmpFile) > 15000000 ){ 
            $contents = '';

            $handle = fopen($tmpFile, "r");
            $counter =0;

            while (!feof($handle)) {
                $contents = fread($handle, 15000000);
                $data = new \MongoDB\BSON\Binary($contents,  \MongoDB\BSON\Binary::TYPE_GENERIC);
                $result = $this->db->$var->insertOne(array('files_id' => $insertedId,'n' => $counter, 'data'=> $data));
                $counter++;
            }

            fclose($handle);
        } else {
            $data = new \MongoDB\BSON\Binary(file_get_contents($file),  \MongoDB\BSON\Binary::TYPE_GENERIC);
            $result = $this->db->$var->insertOne(array('files_id' => $insertedId,'n' => 0, 'data'=> $data));
        }

        if (!is_file($file)) {
            unlink($tmpFile);
        }

        if (empty($result->getInsertedId())) {
            throw new \Exception("update collection $collection GridFS fail: ".print_r($result,true));
        }

        return $insertedId;
    }

    public function fetchFile($collection, $identifier, $returnDocument = false) {

        if (is_array($identifier)) {
            $queryPart = $identifier;
        } else {
            $queryPart = array('_id'=>new \MongoDB\BSON\ObjectID((string)$identifier));
        }

        $var = $collection.'.files';
        $document = $this->db->$var->findOne($queryPart);
        if (empty($document)) {
            return null;
        }
        $var = $collection.'.chunks';
        $documentChunks = $this->db->$var->find(array('files_id'=>new \MongoDB\BSON\ObjectID((string)$document['_id'])))->toArray();

        $fullDocument='';
        foreach($documentChunks as $chunk) {
            $fullDocument .= $chunk['data']->getData();
        }

        if ($returnDocument) {
            return array($document, $fullDocument);
        } else {
            return $fullDocument;
        }
    }


    /*
     * e.g. removeFile('runkeeperGPX', '5dak9kd09aj393ks9');
     */
    public function removeFile($collection, $identifier)
    {
        $var = $collection.'.files';
        $this->db->$var->deleteOne(array('_id'=>new \MongoDB\BSON\ObjectID((string)$identifier)));
        $var = $collection.'.chunks';
        $this->db->$var->deleteOne(array('files_id'=>new \MongoDB\BSON\ObjectID((string)$identifier)));
    }
}
