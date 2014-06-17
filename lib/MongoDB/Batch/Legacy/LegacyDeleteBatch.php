<?php

namespace MongoDB\Batch\Legacy;

use MongoDB\Batch\BatchInterface;
use MongoDB\Exception\UnexpectedTypeException;
use MongoCursorException;
use MongoException;

final class LegacyDeleteBatch extends LegacyWriteBatch
{
    /**
     * @see BatchInterface::add()
     * @throws MongoException if $document does not contain "q" and "limit" keys
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

        if ( ! array_key_exists('limit', $document)) {
            throw new MongoException('Expected $document to contain \'limit\' key');
        }

        if ($document['limit'] != 0 && $document['limit'] != 1) {
            throw new MongoException(sprintf('Excepted \'limit\' to be 0 or 1; given: %d', $document['limit']));
        }

        $document['q'] = (array) $document['q'];
        $document['limit'] = (integer) $document['limit'];

        $this->documents[] = $document;
    }

    /**
     * @see BatchInterface::getType()
     */
    public function getType()
    {
        return BatchInterface::OP_DELETE;
    }

    /**
     * @see LegacyWriteBatch::createEmptyResult()
     */
    protected function createEmptyResult()
    {
        return array(
            'nRemoved' => 0,
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
            $gle = $this->collection->remove($document['q'], array(
                'w' => 1,
                'justOne' => ($document['limit'] === 1),
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

        if ( ! empty($gle['n'])) {
            $result['nRemoved'] += (integer) $gle['n'];
        }
    }
}
