#!/usr/bin/env python3
import argparse
import logging
import sys

from db_updater.database import DatabaseManager
from db_updater.updater import DatabaseUpdater
from db_updater.scheduler import UpdateScheduler


def setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout),
        ]
    )


def show_status():
    db = DatabaseManager()
    info = db.get_table_info()
    
    print("\n" + "=" * 50)
    print("Database Status")
    print("=" * 50)
    
    for table, data in info.items():
        print(f"{table}:")
        print(f"  Rows: {data['rows']:,}")
        print(f"  Tickers: {data['tickers']}")
    print("=" * 50 + "\n")


def main():
    parser = argparse.ArgumentParser(
        description='Database Updater - Daily stock, crypto, and financial data updater'
    )
    
    parser.add_argument(
        '--once',
        action='store_true',
        help='Run update once and exit (default is scheduled mode)'
    )
    parser.add_argument(
        '--scheduled',
        action='store_true',
        help='Run scheduler (default)'
    )
    parser.add_argument(
        '--backfill',
        type=int,
        metavar='DAYS',
        help='Force backfill N days for all tickers'
    )
    parser.add_argument(
        '--ticker',
        type=str,
        metavar='SYMBOL',
        help='Update only a specific ticker'
    )
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Skip backup before update (not recommended)'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be updated without making changes'
    )
    parser.add_argument(
        '--status',
        action='store_true',
        help='Show database status and exit'
    )
    parser.add_argument(
        '-v', '--verbose',
        action='store_true',
        help='Enable verbose logging'
    )
    
    args = parser.parse_args()
    
    setup_logging(args.verbose)
    
    if args.status:
        show_status()
        return
    
    if args.once or args.backfill or args.ticker:
        updater = DatabaseUpdater(
            dry_run=args.dry_run,
            skip_backup=args.no_backup
        )
        
        if args.backfill:
            stats = updater.run(force_backfill_days=args.backfill)
        elif args.ticker:
            stats = updater.run(single_ticker=args.ticker.upper())
        else:
            stats = updater.run()
        
        if stats['errors'] > 0:
            sys.exit(1)
    else:
        scheduler = UpdateScheduler()
        scheduler.start()


if __name__ == '__main__':
    main()
