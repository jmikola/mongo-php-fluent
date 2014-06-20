<?php

namespace MongoDB\Batch\Legacy;

use MongoDB\Batch\BatchInterface;
use MongoDB\Exception\UnexpectedTypeException;
use MongoCursorException;

final class LegacyInsertBatch extends LegacyWriteBatch
{
    /**
     * @see BatchInterface::add()
     * @throws UnexpectedTypeException if $document is neither an array nor an object
     */
    public function add($document)
    {
        if ( ! is_array($document) && ! is_object($document)) {
            throw new UnexpectedTypeException($document, 'array or object');
        }

        $this->documents[] = $document;
    }

    /**
     * @see BatchInterface::getType()
     */
    public function getType()
    {
        return BatchInterface::OP_INSERT;
    }

    /**
     * @see LegacyWriteBatch::createEmptyResult()
     */
    protected function createEmptyResult()
    {
        return array(
            'nInserted' => 0,
            'writeErrors' => array(),
            'writeConcernError' => null,
        );
    }

    /**
     * @see LegacyWriteBatch::executeSingleOperation()
     */
    protected function executeSingleOperation($batchIndex, $document, array $writeConcern, array &$result)
    {
        try {
            $gle = $this->collection->insert($document, $writeConcern);
        } catch (MongoCursorException $e) {
            $gle = $this->db->command(array('getlasterror' => 1));
        }

        if ( ! is_array($gle)) {
            return;
        }

        $err = $this->parseGetLastErrorResponse($gle);

        if ($err['writeConcernError'] !== null && ! isset($result['writeConcernError'])) {
            $result['writeConcernError'] = $err['writeConcernError'];
        }

        if ($err['writeError'] !== null) {
            $result['writeErrors'][] = array(
                'index' => $batchIndex,
                'code' => $err['writeError']['code'],
                'errmsg' => $err['writeError']['errmsg'],
                'op' => $document,
            );

            return;
        }

        $result['nInserted'] += 1;
    }
}
