<?php

namespace MongoDB\Bulk;

use MongoDB\Batch\UnorderedGenerator;
use Iterator;

final class UnorderedBulk extends AbstractBulk
{
    /**
     * Return whether bulk operations are ordered.
     *
     * @see Bulk::isOrdered()
     * @return boolean
     */
    public function isOrdered()
    {
        return false;
    }

    /**
     * Get the batches to be executed.
     *
     * @see Bulk::getBatches()
     * @param array $writeConcern
     * @return Iterator of MappedBatch instances
     */
    protected function getMappedBatches(array $writeConcern = array())
    {
        return new UnorderedGenerator(
            $this->db,
            $this->collection,
            $this->getOperations(),
            $writeConcern
        );
    }
}
