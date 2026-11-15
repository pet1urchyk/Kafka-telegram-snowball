import logging
import os

from project_paths import (
    find_example_env_path,
    resolve_config_path,
    PROJECT_ROOT,
    VAR_ROOT,
)

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:  # pragma: no cover - fallback for optional dependency
    def load_dotenv(*_args, **_kwargs) -> None:
        """Gracefully handle missing python-dotenv dependency."""

        logger.warning(
            "python-dotenv is not installed; skipping automatic loading of .env files."
        )


# Configuration variables with defaults
class Config:
    PROJECT_ROOT_PATH = PROJECT_ROOT
    VAR_ROOT_PATH = VAR_ROOT
    ENV_FILE_PATH = PROJECT_ROOT / ".env"
    EXAMPLE_ENV_FILE_PATH = find_example_env_path()
    LEGACY_PATH_DEFAULTS = {
        'TELEGRAM_SESSION_NAME': 'session_name',
        'RESULTS_FOLDER': 'results',
        'MERGED_FOLDER': 'merged',
        'EDGE_LIST_FOLDER': 'EdgeList',
        'API_DETAILS_FILE': 'api_values.txt',
        'DATA_FOLDER': 'data',
        'NETWORK_ANALYSIS_FOLDER': 'network_analysis',
    }

    # Load environment variables from .env file
    @classmethod
    def reload_env(cls):
        """Reload environment variables from .env file"""
        if cls.ENV_FILE_PATH.exists():
            logger.info("Loading configuration from %s", cls.ENV_FILE_PATH)
            load_dotenv(cls.ENV_FILE_PATH, override=True)
        else:
            logger.warning("No .env file found at %s", cls.ENV_FILE_PATH)

        cls._load_config()
        return cls

    @classmethod
    def _warn_if_legacy_paths_are_configured(cls) -> None:
        """Warn when legacy output path values are still configured."""

        legacy_settings = []
        for key, legacy_value in cls.LEGACY_PATH_DEFAULTS.items():
            if os.getenv(key) == legacy_value:
                legacy_settings.append(f"{key}={legacy_value}")

        if not legacy_settings:
            return

        logger.warning(
            "Legacy path settings detected in %s: %s",
            cls.ENV_FILE_PATH,
            ", ".join(legacy_settings),
        )
        logger.warning(
            "Update these values to the var/... layout from example.env, or remove them to "
            "use the new defaults."
        )

    @classmethod
    def _load_config(cls):
        """Load configuration values from environment variables"""
        cls.PROJECT_ROOT = str(cls.PROJECT_ROOT_PATH)
        cls.ENV_FILE = str(cls.ENV_FILE_PATH)
        cls.EXAMPLE_ENV_FILE = str(cls.EXAMPLE_ENV_FILE_PATH)

        cls._warn_if_legacy_paths_are_configured()

        # Telegram API credentials
        cls.API_ID = os.getenv('TELEGRAM_API_ID')
        cls.API_HASH = os.getenv('TELEGRAM_API_HASH')
        cls.SESSION_NAME = str(
            resolve_config_path(
                os.getenv('TELEGRAM_SESSION_NAME'),
                os.path.join('var', 'sessions', 'session_name'),
            )
        )

        # Log API values for debugging (partial, for security)
        if cls.API_ID:
            logger.info(f"Loaded API_ID from environment: {cls.API_ID}")
        if cls.API_HASH:
            api_hash_masked = cls.API_HASH[:3] + '...' + cls.API_HASH[-3:] if len(cls.API_HASH or '') > 6 else "None"
            logger.info(f"Loaded API_HASH from environment: {api_hash_masked}")

        # Snowball sampling configuration
        cls.DEFAULT_MIN_MENTIONS = int(os.getenv('DEFAULT_MIN_MENTIONS', 5))
        cls.DEFAULT_ITERATIONS = int(os.getenv('DEFAULT_ITERATIONS', 1))
        
        # Ліміти для контролю глибини та обсягу
        cls.DEFAULT_MAX_DEPTH = int(os.getenv('DEFAULT_MAX_DEPTH', 1))
        cls.DEFAULT_MAX_CHANNELS = int(os.getenv('DEFAULT_MAX_CHANNELS', 100))

        cls.DEFAULT_MAX_POSTS = os.getenv('DEFAULT_MAX_POSTS', None)
        if cls.DEFAULT_MAX_POSTS and str(cls.DEFAULT_MAX_POSTS).isdigit():
            cls.DEFAULT_MAX_POSTS = int(cls.DEFAULT_MAX_POSTS)
        else:
            cls.DEFAULT_MAX_POSTS = None

        # Channel recommendations configuration
        cls.DEFAULT_INCLUDE_RECOMMENDATIONS = os.getenv('DEFAULT_INCLUDE_RECOMMENDATIONS', 'True').lower() in ('true', '1', 't')
        cls.DEFAULT_RECOMMENDATIONS_DEPTH = int(os.getenv('DEFAULT_RECOMMENDATIONS_DEPTH', 0))

        # URL extraction configuration
        cls.DEFAULT_INCLUDE_URLS = os.getenv('DEFAULT_INCLUDE_URLS', 'True').lower() in ('true', '1', 't')

        # File paths and directories
        cls.RESULTS_FOLDER = str(
            resolve_config_path(os.getenv('RESULTS_FOLDER'), os.path.join('var', 'runs'))
        )
        cls.MERGED_FOLDER = str(
            resolve_config_path(os.getenv('MERGED_FOLDER'), os.path.join('var', 'merged'))
        )
        cls.EDGE_LIST_FOLDER = str(
            resolve_config_path(
                os.getenv('EDGE_LIST_FOLDER'),
                os.path.join('var', 'edge_lists'),
            )
        )
        cls.EDGE_LIST_FILENAME = os.getenv('EDGE_LIST_FILENAME', 'Edge_List.csv')
        cls.MERGED_FILENAME = os.getenv('MERGED_FILENAME', 'merged_channels.csv')
        cls.API_DETAILS_FILE = str(
            resolve_config_path(
                os.getenv('API_DETAILS_FILE'),
                os.path.join('var', 'api_values.txt'),
            )
        )
        cls.DATA_FOLDER = str(
            resolve_config_path(os.getenv('DATA_FOLDER'), os.path.join('var', 'data'))
        )
        cls.DATABASE_FILENAME = os.getenv('DATABASE_FILENAME', 'telegram_snowball.sqlite')
        cls.DATABASE_PATH = os.path.join(cls.DATA_FOLDER, cls.DATABASE_FILENAME)
        cls.DEFAULT_EDGE_LIST_PATH = os.path.join(
            cls.EDGE_LIST_FOLDER,
            cls.EDGE_LIST_FILENAME,
        )
        cls.NETWORK_ANALYSIS_FOLDER = str(
            resolve_config_path(
                os.getenv('NETWORK_ANALYSIS_FOLDER'),
                os.path.join('var', 'analysis'),
            )
        )

        # Debug mode
        cls.DEBUG = os.getenv('DEBUG', 'False').lower() in ('true', '1', 't')

        # Forward processing configuration
        cls.DEFAULT_INCLUDE_FORWARDS = os.getenv('DEFAULT_INCLUDE_FORWARDS', 'True').lower() in ('true', '1', 't')

        # Checkpoint interval for resumable runs
        cls.CHECKPOINT_INTERVAL = int(os.getenv('CHECKPOINT_INTERVAL', 10))

    @classmethod
    def validate(cls):
        """Validate the configuration and log warnings for missing values"""
        if not cls.API_ID or not cls.API_HASH:
            logger.warning("API credentials not found in .env file")
            return False

        if isinstance(cls.API_ID, str) and cls.API_ID.isdigit():
            cls.API_ID = int(cls.API_ID)
            logger.info("Converted API_ID from string to integer")

        logger.info("Configuration validated successfully")
        logger.info(f"Session name: {cls.SESSION_NAME}")
        logger.info(f"Max depth limit: {cls.DEFAULT_MAX_DEPTH}")
        logger.info(f"Max channels budget: {cls.DEFAULT_MAX_CHANNELS}")
        logger.info(f"Include recommendations: {cls.DEFAULT_INCLUDE_RECOMMENDATIONS}")
        logger.info(f"Recommendations depth: {cls.DEFAULT_RECOMMENDATIONS_DEPTH}")
        logger.info(f"Include URLs: {cls.DEFAULT_INCLUDE_URLS}")
        logger.info(f"Environment file: {cls.ENV_FILE}")
        logger.info(f"Results folder: {cls.RESULTS_FOLDER}")
        logger.info(f"Data folder: {cls.DATA_FOLDER}")
        logger.info(f"Debug mode: {cls.DEBUG}")

        return True


# Initial load of configuration
Config.reload_env()