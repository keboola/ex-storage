<?php

declare(strict_types=1);

namespace Keboola\StorageExtractor;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getToken() : string
    {
        return $this->getValue(['parameters', '#token']);
    }

    public function getUrl() : string
    {
        return $this->getValue(['parameters', 'url']);
    }

    public function getTableName() : string
    {
        return $this->getValue(['parameters', 'tableName']);
    }

    public function getChangedSince() : string
    {
        return (string) $this->getValue(['parameters', 'changedSince']);
    }
}
