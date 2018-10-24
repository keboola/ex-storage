<?php

declare(strict_types=1);

namespace Keboola\StorageExtractor;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getToken(): string
    {
        return $this->getValue(['parameters', '#token']);
    }

    public function getUrl(): string
    {
        return $this->getValue(['parameters', 'url']);
    }

    public function getIncremental(): bool
    {
        return (bool) $this->getValue(['parameters', 'incremental']);
    }

    public function getPrimaryKey(): array
    {
        return (array) $this->getValue(['parameters', 'primaryKey']);
    }

    public function getTableName(): string
    {
        return (string) $this->getValue(['parameters', 'tableName']);
    }

    public function getChangedSince(): string
    {
        return (string) $this->getValue(['parameters', 'changedSince']);
    }

    public function extractMetadata(): bool
    {
        return (bool) $this->getValue(['parameters', 'extractMetadata']);
    }

    public function isFullSync(): bool
    {
        return (bool) $this->getValue(['parameters', 'fullSync']);
    }
}
