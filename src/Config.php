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

    public function getTables() : array
    {
        return $this->getValue(['parameters', 'tables']);
    }
}
