<?php

namespace MongoDB\Bulk;

use MongoDB\Batch\BatchInterface;
use MongoDB\Batch\MappedBatch;

final class BulkResult
{
    public $nInserted = 0;
    public $nMatched = 0;
    public $nModified = 0;
    public $nRemoved = 0;
    public $nUpserted = 0;
    public $writeErrors = array();
    public $writeConcernErrors = array();
    public $upserted = array();

    /**
     * Merges a batch result into the bulk result.
     *
     * @param MappedBatch $batch
     * @param array $batchResult
     */
    public function mergeBatchResult(MappedBatch $batch, array $batchResult)
    {
        /* TODO: If {w:0} was used, the batch result may omit everything except
         * the "ok" field. Ensure that at least "n" is present.
         */

        if ($batch->getType() === BatchInterface::OP_INSERT) {
            $this->nInserted += $batchResult['nInserted'];
        }

        if ($batch->getType() === BatchInterface::OP_DELETE) {
            $this->nRemoved += $batchResult['nRemoved'];
        }

        if ($batch->getType() === BatchInterface::OP_UPDATE) {
            $this->nMatched  += $batchResult['nMatched'];
            $this->nUpserted += $batchResult['nUpserted'];

            // Legacy servers cannot report nModified, so respect null values
            $this->nModified = isset($this->nModified, $batchResult['nModified'])
                ? $this->nModified + $batchResult['nModified']
                : null;

            // Handle case where only a single document was updated
            if (isset($batchResult['upserted']['index'])) {
                $batchResult['upserted'] = array($batchResult['upserted']);
            }

            if ( ! empty($batchResult['upserted']) && is_array($batchResult['upserted'])) {
                foreach ($batchResult['upserted'] as $upsert) {
                    $upsert['index'] = $batch->getBulkIndex($upsert['index']);
                    $this->upserted[] = $upsert;
                }
            }
        }

        if ( ! empty($batchResult['writeErrors']) && is_array($batchResult['writeErrors'])) {
            foreach ($batchResult['writeErrors'] as $writeError) {
                $writeError['index'] = $batch->getBulkIndex($writeError['index']);
                $this->writeErrors[] = $writeError;
            }
        }

        // TODO: writeConcernErrors ultimately need to be aggregated

        if ( ! empty($batchResult['writeConcernError'])) {
            $this->writeConcernErrors[] = $batchResult['writeConcernError'];
        }
    }
}