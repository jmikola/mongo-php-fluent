<?php

namespace MongoDB\Batch\Legacy;

use BadMethodCallException;

final class LegacyDeleteBatch extends LegacyWriteBatch
{
    /**
     * Execute the batch using legacy write operations.
     *
     * @see LegacyWriteBatch::execute()
     * @param array $writeOptions
     * @return array
     * @throws BadMethodCallException if the batch is empty
     */
    public function execute(array $writeOptions = array())
    {
        if (empty($this->documents)) {
            throw new BadMethodCallException('Cannot call execute() for an empty batch');
        }

        $writeOptions = array_merge($this->writeOptions, $writeOptions);

        $result = array(
            'n' => 0,
            'writeErrors' => array(),
            'writeConcernErrors' => array(),
        );

        foreach ($this->documents as $batchIndex => $document) {
            if ($writeOptions['ordered'] && ! empty($result['writeErrors'])) {
                break;
            }

            $gle = $this->collection->remove($document['q'], array(
                'w' => 1,
                'justOne' => ($document['limit'] === 1),
            ));

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
                $result['n'] += (integer) $gle['n'];
            }
        }

        if (empty($result['writeErrors']) ||
            ( ! $writeOptions['ordered'] && count($result['writeErrors']) < count($this->documents))) {

            $err = $this->applyWriteConcern($writeOptions);
        }

        if (isset($err) && $err['wcError'] !== null) {
            $result['writeConcernErrors'][] = $err['wcError'];
        }

        return $result;
    }
}
