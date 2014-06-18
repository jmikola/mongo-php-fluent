<?php

namespace MongoDB\Batch\Legacy;

use MongoDB\Batch\BatchInterface;
use MongoDB\Exception\UnexpectedTypeException;
use MongoCursorException;
use MongoException;
use RuntimeException;

final class LegacyUpdateBatch extends LegacyWriteBatch
{
    /**
     * @see BatchInterface::add()
     * @throws MongoException if $document does not contain "q" and "u" keys
     * @throws UnexpectedTypeException if $document is neither an array nor an object
     */
    public function add($document)
    {
        if (is_object($document)) {
            $document = (array) $document;
        }

        if ( ! is_array($document)) {
            throw new UnexpectedTypeException($document, 'array or object');
        }

        if ( ! array_key_exists('q', $document)) {
            throw new MongoException('Expected $document to contain \'q\' key');
        }

        if ( ! array_key_exists('u', $document)) {
            throw new MongoException('Expected $document to contain \'u\' key');
        }

        $document['q'] = (array) $document['q'];
        $document['u'] = (array) $document['u'];
        $document['multi'] = array_key_exists('multi', $document) ? (boolean) $document['multi'] : false;
        $document['upsert'] = array_key_exists('upsert', $document) ? (boolean) $document['upsert'] : false;

        $this->documents[] = $document;
    }

    /**
     * @see BatchInterface::getType()
     */
    public function getType()
    {
        return BatchInterface::OP_UPDATE;
    }

    /**
     * @see LegacyWriteBatch::createEmptyResult()
     */
    protected function createEmptyResult()
    {
        return array(
            'nMatched' => 0,
            'nModified' => null,
            'nUpserted' => 0,
            'writeErrors' => array(),
            'writeConcernErrors' => array(),
        );
    }

    /**
     * @see LegacyWriteBatch::executeSingleOperation()
     */
    protected function executeSingleOperation($batchIndex, $document, array &$result)
    {
        try {
            $gle = $this->collection->update($document['q'], $document['u'], array(
                'w' => 1,
                'upsert' => $document['upsert'],
                'multiple' => $document['multi'],
            ));
        } catch (MongoCursorException $e) {
            $gle = $this->db->command(array('getlasterror' => 1));
        }

        $err = $this->parseGetLastErrorResponse($gle);

        if ($err['writeError'] !== null) {
            $result['writeErrors'][] = array(
                'index' => $batchIndex,
                'code' => $err['writeError']['code'],
                'errmsg' => $err['writeError']['errmsg'],
                'op' => $document,
            );
        }

        if ($document['upsert'] && empty($gle['updatedExisting'])) {
            $result['nUpserted'] += 1;
            $result['upserted'][] = array(
                'index' => $batchIndex,
                '_id' => $this->getUpsertedId($document, $gle),
            );

            return;
        }

        if ( ! empty($gle['n'])) {
            $result['nMatched'] += (integer) $gle['n'];
        }
    }

    /**
     * Get the identifier for an upsert operation.
     *
     * Checking for "upserted" in the getLastError response is not sufficient
     * for MongoDB 2.4, since the field will not exist unless the identifier was
     * created server-side by the upsert. Therefore, we should also check the
     * update operation's query and newObj documents.
     *
     * @see LegacyUpdateBatch::execute()
     * @see https://jira.mongodb.org/browse/DOCS-2589
     * @see https://jira.mongodb.org/browse/PHP-1116
     * @param array $operation Update operation
     * @param array $gle       getLastError response
     * @return mixed
     * @throws RuntimeException if the upserted identifier cannot be determined
     */
    private function getUpsertedId(array $operation, array $gle)
    {
        if (isset($gle['upserted']) || array_key_exists('upserted', $gle)) {
            return $gle['upserted'];
        }

        if (isset($operation['u']['_id']) || array_key_exists('_id', $operation['u'])) {
            return $operation['u']['_id'];
        }

        if (isset($operation['q']['_id']) || array_key_exists('_id', $operation['q'])) {
            return $operation['q']['_id'];
        }

        throw new RuntimeException('Could not determine upserted identifier');
    }
}
