import unittest
from unittest.mock import patch, MagicMock, call
import os
import sys
import shutil
import subprocess
from pathlib import Path

# Add parent directory to path to import the script
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Mock snowflake.connector before importing the script if possible, 
# but since we import it inside the function, we can mock it in the tests.
# However, to avoid ModuleNotFoundError during the import inside the function,
# we need to make sure sys.modules has 'snowflake.connector'.

import sync_snowflake_packages

class TestSyncSnowflakePackages(unittest.TestCase):

    def setUp(self):
        # Setup common test data
        self.requirements_path = "requirements.txt"
        self.download_dir = "./test_downloads"
        self.zip_name = "test_packages.zip"
        self.stage_name = "@TEST_DB.TEST_SCHEMA.TEST_STAGE"
        self.proget_url = "https://proget.example.com/pypi/test-feed/simple"

    @patch('sync_snowflake_packages.subprocess.check_call')
    @patch('sync_snowflake_packages.shutil.rmtree')
    @patch('sync_snowflake_packages.os.makedirs')
    @patch('sync_snowflake_packages.os.path.exists')
    def test_download_packages(self, mock_exists, mock_makedirs, mock_rmtree, mock_subprocess):
        # Setup mocks
        mock_exists.return_value = True # Simulate directory exists to test cleanup

        # Execute
        sync_snowflake_packages.download_packages(self.requirements_path, self.download_dir, self.proget_url)

        # Verify cleanup of existing dir
        mock_rmtree.assert_called_once_with(self.download_dir)
        
        # Verify creation of new dir
        mock_makedirs.assert_called_once_with(self.download_dir)

        # Verify pip command
        expected_cmd = [
            sys.executable, "-m", "pip", "download",
            "-r", self.requirements_path,
            "-d", self.download_dir,
            "--platform", "manylinux2014_x86_64",
            "--only-binary=:all:",
            "--python-version", "3.8",
            "--index-url", self.proget_url
        ]
        mock_subprocess.assert_called_once_with(expected_cmd)

    @patch('sync_snowflake_packages.subprocess.check_call')
    def test_download_packages_failure(self, mock_subprocess):
        # Setup mock to raise exception
        mock_subprocess.side_effect = subprocess.CalledProcessError(1, "pip download")

        # Execute and verify exception
        with self.assertRaises(subprocess.CalledProcessError):
            sync_snowflake_packages.download_packages(self.requirements_path, self.download_dir)

    @patch('sync_snowflake_packages.zipfile.ZipFile')
    @patch('sync_snowflake_packages.os.walk')
    def test_create_zip(self, mock_walk, mock_zipfile):
        # Setup mocks
        mock_walk.return_value = [
            ('/root', ('dir',), ('file1.txt', 'file2.py')),
        ]
        mock_zip_context = MagicMock()
        mock_zipfile.return_value.__enter__.return_value = mock_zip_context

        # Execute
        sync_snowflake_packages.create_zip(self.download_dir, self.zip_name)

        # Verify ZipFile opened correctly
        mock_zipfile.assert_called_once_with(self.zip_name, 'w', sync_snowflake_packages.zipfile.ZIP_DEFLATED)

        # Verify files written
        self.assertEqual(mock_zip_context.write.call_count, 2)

    def test_upload_to_snowflake(self):
        # Mock snowflake.connector in sys.modules
        mock_snowflake = MagicMock()
        mock_connector = MagicMock()
        mock_snowflake.connector = mock_connector
        
        with patch.dict(sys.modules, {'snowflake': mock_snowflake, 'snowflake.connector': mock_connector}):
            # We also need to patch environment variables
            with patch.dict(os.environ, {
                "SNOWFLAKE_ACCOUNT": "acc",
                "SNOWFLAKE_USER": "usr",
                "SNOWFLAKE_PASSWORD": "pwd",
                "SNOWFLAKE_ROLE": "role",
                "SNOWFLAKE_WAREHOUSE": "wh",
                "SNOWFLAKE_DATABASE": "db",
                "SNOWFLAKE_SCHEMA": "sch"
            }):
                # Setup connection mock
                mock_conn = MagicMock()
                mock_cursor = MagicMock()
                mock_connector.connect.return_value = mock_conn
                mock_conn.cursor.return_value = mock_cursor
                mock_cursor.fetchall.return_value = [('file.zip', 'UPLOADED')]

                # Execute
                sync_snowflake_packages.upload_to_snowflake(self.zip_name, self.stage_name)

                # Verify connection
                mock_connector.connect.assert_called_once_with(
                    account="acc", user="usr", password="pwd",
                    role="role", warehouse="wh", database="db", schema="sch"
                )

                # Verify PUT command
                abs_path = os.path.abspath(self.zip_name)
                expected_put = f"PUT file://{abs_path} {self.stage_name} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                mock_cursor.execute.assert_called_once_with(expected_put)

                # Verify connection closed
                mock_conn.close.assert_called_once()

    def test_upload_to_snowflake_missing_creds(self):
        # Mock snowflake.connector so import works
        mock_snowflake = MagicMock()
        mock_connector = MagicMock()
        mock_snowflake.connector = mock_connector
        
        with patch.dict(sys.modules, {'snowflake': mock_snowflake, 'snowflake.connector': mock_connector}):
            with patch.dict(os.environ, {}, clear=True):
                # Execute and verify exception
                with self.assertRaises(ValueError):
                    sync_snowflake_packages.upload_to_snowflake(self.zip_name, self.stage_name)

    @patch('sync_snowflake_packages.upload_to_snowflake')
    @patch('sync_snowflake_packages.create_zip')
    @patch('sync_snowflake_packages.download_packages')
    @patch('sync_snowflake_packages.setup_args')
    @patch('sync_snowflake_packages.shutil.rmtree')
    @patch('sync_snowflake_packages.os.remove')
    @patch('sync_snowflake_packages.os.path.exists')
    def test_main_success(self, mock_exists, mock_remove, mock_rmtree, 
                          mock_setup_args, mock_download, mock_zip, mock_upload):
        # Setup mocks
        mock_args = MagicMock()
        mock_args.requirements = "reqs.txt"
        mock_args.download_dir = "dl_dir"
        mock_args.zip_name = "pkg.zip"
        mock_args.stage = "stage"
        mock_args.proget_url = "url"
        mock_setup_args.return_value = mock_args
        
        mock_exists.return_value = True # For cleanup

        # Execute
        sync_snowflake_packages.main()

        # Verify sequence
        mock_download.assert_called_once_with("reqs.txt", "dl_dir", "url")
        mock_zip.assert_called_once_with("dl_dir", "pkg.zip")
        mock_upload.assert_called_once_with("pkg.zip", "stage")
        
        # Verify cleanup
        mock_rmtree.assert_called_once_with("dl_dir")
        mock_remove.assert_called_once_with("pkg.zip")

if __name__ == '__main__':
    unittest.main()
