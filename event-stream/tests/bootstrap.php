<?php

declare(strict_types=1);

/**
 * @author  Luki Centuri <lukicenturi@gmail.com>
 */

$packageVendor = dirname(__DIR__) . '/vendor/autoload.php';
$monorepoVendor = dirname(__DIR__, 2) . '/vendor/autoload.php';

if (file_exists($packageVendor)) {
    require_once $packageVendor;
} elseif (file_exists($monorepoVendor)) {
    require_once $monorepoVendor;
} else {
    fwrite(STDERR, 'Run `composer install` before executing the test suite.' . PHP_EOL);
    exit(1);
}

spl_autoload_register(static function (string $class): void {
    $roots = [
        'Menumbing\\EventStream\\Tests\\' => __DIR__ . '/',
        'Menumbing\\EventStream\\' => dirname(__DIR__) . '/src/',
    ];

    foreach ($roots as $prefix => $directory) {
        if (str_starts_with($class, $prefix)) {
            $path = $directory . str_replace('\\', '/', substr($class, strlen($prefix))) . '.php';

            if (is_file($path)) {
                require_once $path;
            }

            return;
        }
    }
});

! defined('BASE_PATH') && define('BASE_PATH', dirname(__DIR__));

date_default_timezone_set('UTC');
