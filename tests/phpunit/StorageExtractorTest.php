<?php

declare(strict_types=1);

namespace Keboola\StorageExtractor\Tests;

use Exception;
use Keboola\Component\UserException;
use Keboola\Csv\CsvFile;
use Keboola\StorageApi\Client;
use Keboola\StorageApi\Metadata;
use Keboola\StorageExtractor\Component;
use Keboola\Temp\Temp;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\Filesystem\Filesystem;

class StorageExtractorTest extends TestCase
{
    private Client $client;

    public function setUp(): void
    {
        parent::setUp();
        if (empty(getenv('KBC_TEST_TOKEN')) || empty(getenv('KBC_TEST_WRITE_TOKEN')) ||
                empty(getenv('KBC_TEST_URL')) || empty(getenv('KBC_TEST_BUCKET'))
        ) {
            throw new Exception('KBC_TEST_TOKEN, KBC_TEST_WRITE_TOKEN, KBC_TEST_URL or KBC_TEST_BUCKET is empty');
        }
        echo 'Test token: ' . substr((string) getenv('KBC_TEST_TOKEN'), 0, 10);
        echo 'Write  token: ' . substr((string) getenv('KBC_TEST_WRITE_TOKEN'), 0, 10);
        $this->client = new Client([
            'token' => getenv('KBC_TEST_WRITE_TOKEN'),
            'url' => getenv('KBC_TEST_URL'),
        ]);
        $tables = $this->client->listTables((string) getenv('KBC_TEST_BUCKET'));
        foreach ($tables as $table) {
            $this->client->dropTable($table['id']);
        }
    }

    public function testBasic(): void
    {
        $temp = new Temp('ex-storage');
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"name\"\n\"1\",\"a\"\n\"2\",\"b\"\n\"3\",\"c\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-1', $csv, ['primaryKey' => 'id']);

        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'tableName' => 'some-table-1',
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->execute();
        self::assertFileExists($baseDir . '/out/tables/some-table-1.csv');
        $csv = new CsvFile($baseDir . '/out/tables/some-table-1.csv');
        $rows = iterator_to_array($csv);
        sort($rows);
        self::assertEquals(
            [
                ['1', 'a'],
                ['2', 'b'],
                ['3', 'c'],
                ['id', 'name'],
            ],
            $rows
        );
        self::assertFileExists($baseDir . '/out/tables/some-table-1.csv.manifest');
        $data = (string) json_decode(
            (string) file_get_contents($baseDir . '/out/tables/some-table-1.csv.manifest'),
            true
        );
        self::assertEquals(['primary_key' => [], 'incremental' => false], $data);
    }

    public function testInvalidToken(): void
    {
        $temp = new Temp('ex-storage');
        $fs = new Filesystem();
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => 'invalid',
                'url' => getenv('KBC_TEST_URL'),
                'tableName' => 'invalid',
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage('Invalid access token');
        $app->execute();
    }

    public function testInvalidTokenPermissions(): void
    {
        $temp = new Temp('ex-storage');
        $fs = new Filesystem();
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_WRITE_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'tableName' => 'invalid',
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'The token must have read-only permissions to the bucket "' . getenv('KBC_TEST_BUCKET') . '".'
        );
        $app->execute();
    }

    public function testTableNoneExistent(): void
    {
        $temp = new Temp('ex-storage');
        $fs = new Filesystem();
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'tableName' => 'non-existent-table',
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage(
            'The table "non-existent-table" was not found in the bucket "' . getenv('KBC_TEST_BUCKET') . '"'
        );
        $app->execute();
    }

    public function testTableMissing(): void
    {
        $temp = new Temp('ex-storage');
        $fs = new Filesystem();
        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage('The tableName parameter must be provided.');
        $app->execute();
    }

    public function testTableMetadata(): void
    {
        $temp = new Temp('ex-storage');
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"foo\"\n\"1\",\"a\"\n\"2\",\"b\"\n\"3\",\"c\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-4', $csv, ['primaryKey' => 'id']);
        $metadata = new Metadata($this->client);
        $metadata->postTableMetadata(
            getenv('KBC_TEST_BUCKET') . '.some-table-4',
            'testing',
            [
                [
                    'key' => 'some-key',
                    'value' => 'some-value',
                ],
            ]
        );
        $metadata->postColumnMetadata(
            getenv('KBC_TEST_BUCKET') . '.some-table-4.foo',
            'testing',
            [
                [
                    'key' => 'another-key',
                    'value' => 'another-value',
                    'provider' => 'test',
                ],
            ]
        );

        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'tableName' => 'some-table-4',
                'extractMetadata' => true,
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->execute();
        self::assertFileExists($baseDir . '/out/tables/some-table-4.csv');
        $csv = new CsvFile($baseDir . '/out/tables/some-table-4.csv');
        $rows = iterator_to_array($csv);
        sort($rows);
        self::assertEquals(
            [
                ['1', 'a'],
                ['2', 'b'],
                ['3', 'c'],
                ['id', 'foo'],
            ],
            $rows
        );
        self::assertFileExists($baseDir . '/out/tables/some-table-4.csv.manifest');
        $data = (string) json_decode(
            (string) file_get_contents($baseDir . '/out/tables/some-table-4.csv.manifest'),
            true
        );
        self::assertEquals(
            [
                'metadata' => [
                    [
                        'key' => 'some-key',
                        'value' => 'some-value',
                    ],
                ],
                'column_metadata' => [
                    'foo' => [
                        [
                            'key' => 'another-key',
                            'value' => 'another-value',
                        ],
                    ],
                ],
                'primary_key' => [],
                'incremental' => false,
            ],
            $data
        );
    }

    public function testActionSourceInfo(): void
    {
        $temp = new Temp('ex-storage');
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"foo\"\n\"1\",\"aa\"\n\"2\",\"bb\"\n\"3\",\"cc\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-5', $csv, ['primaryKey' => 'id']);
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"bar\"\n\"1\",\"x\"\n\"2\",\"y\"\n\"3\",\"z\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-6', $csv, ['primaryKey' => 'id']);

        $configFile = [
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
            ],
            'action' => 'sourceInfo',
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $result = '';
        ob_start(function ($content) use (&$result): void {
            $result .= $content;
        });
        $app->execute();
        ob_end_clean();
        $data = json_decode($result, true);
        self::assertArrayHasKey('tables', $data);
        ksort($data['tables']);
        self::assertEquals(
            [
                [
                    'name' => 'some-table-5',
                    'primaryKey' => [
                        'id',
                    ],
                ],
                [
                    'name' => 'some-table-6',
                    'primaryKey' => [
                        'id',
                    ],
                ],
            ],
            $data['tables']
        );
        $client = new Client(['token' => getenv('KBC_TEST_TOKEN'), 'url' => getenv('KBC_TEST_URL')]);
        $tokenInfo = $client->verifyToken();
        self::assertArrayHasKey('project', $data);
        self::assertEquals($tokenInfo['owner']['id'], $data['project']['projectId']);
        self::assertEquals($tokenInfo['owner']['name'], $data['project']['projectName']);
    }

    public function testActionInvalidToken(): void
    {
        $temp = new Temp('ex-storage');
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();

        $configFile = [
            'parameters' => [
                '#token' => 'invalid',
                'url' => getenv('KBC_TEST_URL'),
            ],
            'action' => 'list',
        ];
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        self::expectException(UserException::class);
        self::expectExceptionMessage('Invalid access token');
        $app->execute();
    }

    public function testChangedSince(): void
    {
        $temp = new Temp('ex-storage');
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"name\"\n\"1\",\"a\"\n\"2\",\"b\"\n\"3\",\"c\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-1', $csv, ['primaryKey' => 'id']);
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"name\"\n\"5\",\"x\"\n\"6\",\"y\"\n");
        $timestamp = time();
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->writeTable(getenv('KBC_TEST_BUCKET') . '.some-table-1', $csv, ['incremental' => true]);

        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'tableName' => 'some-table-1',
                'changedSince' => $timestamp,
                'incremental' => true,
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->execute();
        self::assertFileExists($baseDir . '/out/tables/some-table-1.csv');
        $csv = new CsvFile($baseDir . '/out/tables/some-table-1.csv');
        $rows = iterator_to_array($csv);
        sort($rows);
        self::assertEquals(
            [
                ['5', 'x'],
                ['6', 'y'],
                ['id', 'name'],
            ],
            $rows
        );
        self::assertFileExists($baseDir . '/out/tables/some-table-1.csv.manifest');
        $data = (string) json_decode(
            (string) file_get_contents($baseDir . '/out/tables/some-table-1.csv.manifest'),
            true
        );
        self::assertEquals(['primary_key' => [], 'incremental' => true], $data);
    }

    public function testPrimaryKeyFullSync(): void
    {
        $temp = new Temp('ex-storage');
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"name\"\n\"1\",\"a\"\n\"2\",\"b\"\n\"3\",\"c\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-1', $csv, ['primaryKey' => 'id']);

        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'tableName' => 'some-table-1',
                'fullSync' => true,
                'primaryKey' => ['ignored', 'garbage'], // ignored
                'incremental' => true, //ignored
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->execute();
        self::assertFileExists($baseDir . '/out/tables/some-table-1.csv');
        $csv = new CsvFile($baseDir . '/out/tables/some-table-1.csv');
        $rows = iterator_to_array($csv);
        sort($rows);
        self::assertEquals(
            [
                ['1', 'a'],
                ['2', 'b'],
                ['3', 'c'],
                ['id', 'name'],
            ],
            $rows
        );
        self::assertFileExists($baseDir . '/out/tables/some-table-1.csv.manifest');
        $data = (string) json_decode(
            (string) file_get_contents($baseDir . '/out/tables/some-table-1.csv.manifest'),
            true
        );
        self::assertEquals(['primary_key' => ['id'], 'incremental' => false], $data);
    }

    public function testPrimaryKeyExplicit(): void
    {
        $temp = new Temp('ex-storage');
        $fs = new Filesystem();
        $fs->dumpFile($temp->getTmpFolder() . '/tmp.csv', "\"id\",\"name\"\n\"1\",\"a\"\n\"2\",\"b\"\n\"3\",\"c\"\n");
        $csv = new CsvFile($temp->getTmpFolder() . '/tmp.csv');
        $this->client->createTable(getenv('KBC_TEST_BUCKET'), 'some-table-1', $csv, ['primaryKey' => 'id']);

        $configFile = [
            'action' => 'run',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
                'tableName' => 'some-table-1',
                'primaryKey' => ['name', 'id'],
            ],
        ];
        $baseDir = $temp->getTmpFolder();
        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $app->execute();
        self::assertFileExists($baseDir . '/out/tables/some-table-1.csv');
        $csv = new CsvFile($baseDir . '/out/tables/some-table-1.csv');
        $rows = iterator_to_array($csv);
        sort($rows);
        self::assertEquals(
            [
                ['1', 'a'],
                ['2', 'b'],
                ['3', 'c'],
                ['id', 'name'],
            ],
            $rows
        );
        self::assertFileExists($baseDir . '/out/tables/some-table-1.csv.manifest');
        $data = (string) json_decode(
            (string) file_get_contents($baseDir . '/out/tables/some-table-1.csv.manifest'),
            true
        );
        self::assertEquals(['primary_key' => ['name', 'id'], 'incremental' => false], $data);
    }

    public function testInfoAction(): void
    {
        $temp = new Temp('ex-storage');
        $baseDir = $temp->getTmpFolder();
        $fs = new Filesystem();

        $configFile = [
            'action' => 'info',
            'parameters' => [
                '#token' => getenv('KBC_TEST_TOKEN'),
                'url' => getenv('KBC_TEST_URL'),
            ],
        ];

        $fs->dumpFile($baseDir . '/config.json', (string) json_encode($configFile));
        putenv('KBC_DATADIR=' . $baseDir);
        $app = new Component(new NullLogger());
        $result = '';
        ob_start(function ($content) use (&$result): void {
            $result .= $content;
        });
        $app->execute();
        ob_end_clean();

        $decodeResult = json_decode($result, true);
        $tokenInfo = $this->client->verifyToken();

        self::assertArrayHasKey('projectId', $decodeResult);
        self::assertArrayHasKey('projectName', $decodeResult);
        self::assertArrayHasKey('bucket', $decodeResult);

        self::assertEquals(getenv('KBC_TEST_BUCKET'), $decodeResult['bucket']);
        self::assertEquals($tokenInfo['owner']['id'], $decodeResult['projectId']);
        self::assertEquals($tokenInfo['owner']['name'], $decodeResult['projectName']);
    }
}
