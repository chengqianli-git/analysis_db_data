#!/usr/bin/env python3
"""
for getting data statistics, for generating simulated data
"""

import json
import os
from datetime import datetime
from decimal import Decimal
import traceback
from typing import Dict, List, Any
import pymysql
from pymysql.cursors import DictCursor


class DecimalEncoder(json.JSONEncoder):
    """custom json encoder for decimal type"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


class ProductionDataProfiler:
    """production environment data feature analyzer"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        :param config: database connection configuration
        """
        self.config = config
        self.connection = None
        self.results = {
            'metadata': {
                'analysis_date': datetime.now().isoformat(),
                'database': config.get('database', 'unknown')
            },
            'tables': {}
        }
        
        # define the core tables to analyze
        self.core_tables = [
            'account_base',
            'person_norm',
            'activity',
            'account_list_member',
            'account_to_company_mappings',
            'predictive_model_account_scores'
        ]
        
        # configuration options
        self.max_columns_to_analyze = config.get('max_columns_to_analyze', 50)
        
        # large table specific optimization strategies
        self.large_table_configs = {
            'activity': {
                'sample_rate': config.get('activity_sample_rate', 0.01),  # 1% sampling
                'time_range_days': config.get('activity_time_range_days', 90),  # last 90 days
                'time_column': 'activity_date'
            }
        }
    
    def connect(self):
        """connect to database"""
        try:
            self.connection = pymysql.connect(
                host=self.config['host'],
                port=self.config.get('port', 3306),
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                cursorclass=DictCursor
            )
            print(f"✓ connect to database: {self.config['database']}")
        except Exception as e:
            print(f"✗ connect to database failed: {e}, {traceback.format_exc()}")
            raise
    
    def close(self):
        """close database connection"""
        if self.connection:
            self.connection.close()
            print("✓ database connection closed")
    
    def execute_query(self, query: str) -> List[Dict]:
        """execute sql query and return result"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            print(f"✗ execute query failed: {e}, {traceback.format_exc()}")
            print(f"SQL: {query}")
            return []
    
    def get_table_row_count(self, table_name: str, where_clause: str = "") -> int:
        """get table row count"""
        query = f"SELECT COUNT(*) as count FROM `{table_name}`"
        if where_clause:
            query += f" WHERE {where_clause}"
        result = self.execute_query(query)
        return result[0]['count'] if result else 0
    
    def get_table_columns(self, table_name: str) -> List[Dict]:
        """get table columns"""
        query = f"""
        SELECT 
            COLUMN_NAME as column_name,
            DATA_TYPE as data_type,
            IS_NULLABLE as is_nullable,
            COLUMN_DEFAULT as column_default,
            CHARACTER_MAXIMUM_LENGTH as max_length,
            NUMERIC_PRECISION as numeric_precision,
            NUMERIC_SCALE as numeric_scale,
            COLUMN_TYPE as column_type,
            COLUMN_KEY as column_key
        FROM information_schema.COLUMNS
        WHERE TABLE_SCHEMA = '{self.config['database']}'
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        return self.execute_query(query)
    
    def _build_where_clause(self, table_name: str, optimization_config: Dict = None) -> str:
        """build WHERE clause for large table optimization"""
        if not optimization_config:
            return ""
        
        conditions = []
        
        # add time range filter
        if 'time_column' in optimization_config and 'time_range_days' in optimization_config:
            time_column = optimization_config['time_column']
            days = optimization_config['time_range_days']
            conditions.append(f"`{time_column}` >= DATE_SUB(NOW(), INTERVAL {days} DAY)")
        
        # add sampling filter
        if 'sample_rate' in optimization_config:
            sample_rate = optimization_config['sample_rate']
            conditions.append(f"RAND() < {sample_rate}")
        
        return f"WHERE {' AND '.join(conditions)}" if conditions else ""
    
    def analyze_numeric_column(self, table_name: str, column_name: str, optimization_config: Dict = None) -> Dict:
        """analyze numeric column statistics"""
        where_clause = self._build_where_clause(table_name, optimization_config)
        
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(`{column_name}`) as non_null_count,
            APPROX_COUNT_DISTINCT(`{column_name}`) as unique_count,
            MIN(`{column_name}`) as min_value,
            MAX(`{column_name}`) as max_value,
            AVG(`{column_name}`) as avg_value,
            STDDEV(`{column_name}`) as std_dev
        FROM `{table_name}`
        {where_clause}
        """
        result = self.execute_query(query)
        if result:
            data = result[0]
            stats = {
                'total_count': data['total_count'],
                'non_null_count': data['non_null_count'],
                'null_count': data['total_count'] - data['non_null_count'],
                'null_percentage': round((data['total_count'] - data['non_null_count']) * 100.0 / data['total_count'], 2) if data['total_count'] > 0 else 0,
                'unique_count': data['unique_count'],
                'unique_percentage': round(data['unique_count'] * 100.0 / data['non_null_count'], 2) if data['non_null_count'] > 0 else 0,
                'min_value': float(data['min_value']) if data['min_value'] is not None else None,
                'max_value': float(data['max_value']) if data['max_value'] is not None else None,
                'avg_value': round(float(data['avg_value']), 4) if data['avg_value'] is not None else None,
                'std_dev': round(float(data['std_dev']), 4) if data['std_dev'] is not None else None
            }
            
            # add optimization metadata if applied
            if optimization_config:
                stats['optimization_applied'] = {
                    'sample_rate': optimization_config.get('sample_rate'),
                    'time_range_days': optimization_config.get('time_range_days'),
                    'method': 'sampling + time_range' if 'sample_rate' in optimization_config and 'time_range_days' in optimization_config else 'time_range' if 'time_range_days' in optimization_config else 'sampling'
                }
            
            return stats
        return {}
    
    def analyze_string_column(self, table_name: str, column_name: str, optimization_config: Dict = None) -> Dict:
        """analyze string column statistics"""
        where_clause = self._build_where_clause(table_name, optimization_config)
        
        # basic statistics
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(`{column_name}`) as non_null_count,
            APPROX_COUNT_DISTINCT(`{column_name}`) as unique_count,
            AVG(LENGTH(`{column_name}`)) as avg_length,
            MIN(LENGTH(`{column_name}`)) as min_length,
            MAX(LENGTH(`{column_name}`)) as max_length
        FROM `{table_name}`
        {where_clause}
        """
        result = self.execute_query(query)
        
        stats = {}
        if result:
            data = result[0]
            total_rows = self.get_table_row_count(table_name, where_clause)
            stats = {
                'total_count': total_rows,
                'non_null_count': data['non_null_count'],
                'null_count': total_rows - data['non_null_count'],
                'null_percentage': round((total_rows - data['non_null_count']) * 100.0 / total_rows, 2) if total_rows > 0 else 0,
                'unique_count': data['unique_count'],
                'unique_percentage': round(data['unique_count'] * 100.0 / data['non_null_count'], 2) if data['non_null_count'] > 0 else 0,
                'avg_length': round(float(data['avg_length']), 2) if data['avg_length'] is not None else None,
                'min_length': data['min_length'],
                'max_length': data['max_length']
            }
            
            # add optimization metadata if applied
            if optimization_config:
                stats['optimization_applied'] = {
                    'sample_rate': optimization_config.get('sample_rate'),
                    'time_range_days': optimization_config.get('time_range_days')
                } 
        
        # if the unique value is less (probably a categorical column), get the distribution
        if stats.get('unique_count', float('inf')) < 100 and stats.get('unique_count', 0) > 0:
            dist_query = f"""
            SELECT 
                `{column_name}` as value,
                COUNT(*) as frequency,
                COUNT(*) * 100.0 / (SELECT COUNT(*) FROM `{table_name}`) as percentage
            FROM `{table_name}`
            {where_clause if where_clause else f"WHERE `{column_name}` IS NOT NULL"}
            GROUP BY `{column_name}`
            ORDER BY frequency DESC
            LIMIT 50
            """
            distribution = self.execute_query(dist_query)
            stats['value_distribution'] = [
                {
                    'value_type': type(d['value']).__name__,  # do not save the actual value, only save the type
                    'frequency': d['frequency'],
                    'percentage': round(d['percentage'], 2)
                }
                for d in distribution
            ]
        
        return stats
    
    def analyze_datetime_column(self, table_name: str, column_name: str, optimization_config: Dict = None) -> Dict:
        """analyze datetime column statistics"""
        where_clause = self._build_where_clause(table_name, optimization_config)
        
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(`{column_name}`) as non_null_count,
            MIN(`{column_name}`) as min_date,
            MAX(`{column_name}`) as max_date
        FROM `{table_name}`
        {where_clause}
        """
        result = self.execute_query(query)
        
        if result:
            data = result[0]
            stats = {
                'total_count': data['total_count'],
                'non_null_count': data['non_null_count'],
                'null_count': data['total_count'] - data['non_null_count'],
                'null_percentage': round((data['total_count'] - data['non_null_count']) * 100.0 / data['total_count'], 2) if data['total_count'] > 0 else 0,
                'min_date': data['min_date'].isoformat() if data['min_date'] else None,
                'max_date': data['max_date'].isoformat() if data['max_date'] else None
            }
            
            # calculate date range (unit: days)
            if data['min_date'] and data['max_date']:
                date_range = (data['max_date'] - data['min_date']).days
                stats['date_range_days'] = date_range
            
            dist_query = f"""
            SELECT 
                YEAR(`{column_name}`) as year,
                MONTH(`{column_name}`) as month,
                COUNT(*) as count
            FROM `{table_name}`
            {where_clause if where_clause else f"WHERE `{column_name}` IS NOT NULL"}
            GROUP BY YEAR(`{column_name}`), MONTH(`{column_name}`)
            ORDER BY year DESC, month DESC
            LIMIT 24
            """
            time_dist = self.execute_query(dist_query)
            stats['monthly_distribution'] = [
                {'year': d['year'], 'month': d['month'], 'count': d['count']}
                for d in time_dist
            ]
            
            # add optimization metadata if applied
            if optimization_config:
                stats['optimization_applied'] = {
                    'sample_rate': optimization_config.get('sample_rate'),
                    'time_range_days': optimization_config.get('time_range_days')
                }
            
            return stats
        return {}
    
    def analyze_json_column(self, table_name: str, column_name: str, optimization_config: Dict = None) -> Dict:
        """analyze json column statistics"""
        where_clause = self._build_where_clause(table_name, optimization_config)
        
        query = f"""
        SELECT 
            COUNT(*) as total_count,
            COUNT(`{column_name}`) as non_null_count,
            AVG(LENGTH(`{column_name}`)) as avg_length
        FROM `{table_name}`
        {where_clause}
        """
        result = self.execute_query(query)
        
        if result:
            data = result[0]
            stats = {
                'total_count': data['total_count'],
                'non_null_count': data['non_null_count'],
                'null_count': data['total_count'] - data['non_null_count'],
                'null_percentage': round((data['total_count'] - data['non_null_count']) * 100.0 / data['total_count'], 2) if data['total_count'] > 0 else 0,
                'avg_length': round(float(data['avg_length']), 2) if data['avg_length'] is not None else None
            }
            
            # add optimization metadata if applied
            if optimization_config:
                stats['optimization_applied'] = {
                    'sample_rate': optimization_config.get('sample_rate'),
                    'time_range_days': optimization_config.get('time_range_days')
                } 
            
            return stats
        return {}
    
    def analyze_column(self, table_name: str, column_info: Dict, optimization_config: Dict = None) -> Dict:
        """analyze column by data type"""
        column_name = column_info['column_name']
        data_type = column_info['data_type'].lower()
        
        opt_info = ""
        if optimization_config:
            opt_info = f" [optimized: {optimization_config.get('sample_rate', 0)*100:.1f}% sample, {optimization_config.get('time_range_days', 'N/A')} days]"
        print(f"  - analyze column: {column_name} ({data_type}){opt_info}")
        
        if data_type in ['int', 'bigint', 'tinyint', 'smallint', 'mediumint', 'decimal', 'float', 'double']:
            return self.analyze_numeric_column(table_name, column_name, optimization_config)
        elif data_type in ['varchar', 'char', 'text', 'mediumtext', 'longtext']:
            return self.analyze_string_column(table_name, column_name, optimization_config)
        elif data_type in ['datetime', 'date', 'timestamp']:
            return self.analyze_datetime_column(table_name, column_name, optimization_config)
        elif data_type == 'json':
            return self.analyze_json_column(table_name, column_name, optimization_config)
        elif data_type == 'boolean':
            # Boolean column can be treated as a categorical column
            return self.analyze_string_column(table_name, column_name, optimization_config)
        else:
            where_clause = self._build_where_clause(table_name, optimization_config)
            # other types only get basic statistics
            query = f"""
            SELECT 
                COUNT(*) as total_count,
                COUNT(`{column_name}`) as non_null_count
            FROM `{table_name}`
            {where_clause if where_clause else f"WHERE `{column_name}` IS NOT NULL"}
            """
            result = self.execute_query(query)
            if result:
                data = result[0]
                return {
                    'total_count': data['total_count'],
                    'non_null_count': data['non_null_count'],
                    'null_count': data['total_count'] - data['non_null_count'],
                    'null_percentage': round((data['total_count'] - data['non_null_count']) * 100.0 / data['total_count'], 2) if data['total_count'] > 0 else 0
                }
        return {}
    
    def analyze_table(self, table_name: str):
        """analyze all features of a single table"""
        print(f"\n" + "*"*30 + " begin to analyze table: " + table_name + " " + "*"*30)
        
        # check if large table optimization should be applied
        optimization_config = None
        is_large_table = False
        
        if table_name in self.large_table_configs:
            # enable large table optimization for configured tables
            is_large_table = True
            optimization_config = self.large_table_configs[table_name]
            print(f"  enabling optimization: {optimization_config.get('sample_rate', 0)*100:.1f}% sampling + {optimization_config.get('time_range_days', 'N/A')} days time range")
        
        # get table columns
        columns = self.get_table_columns(table_name)
        print(f"✓ table column count: {len(columns)}")
        
        # filter columns if large table
        columns_to_analyze = columns
        if is_large_table and 'key_columns' in optimization_config:
            # filter columns if large table
            key_columns = optimization_config['key_columns']
            columns_to_analyze = [col for col in columns if col['column_name'] in key_columns]
            print(f"   analyzing {len(columns_to_analyze)} key columns instead of all {len(columns)} columns")
        
        # limit columns to analyze
        columns_to_analyze = columns_to_analyze[:self.max_columns_to_analyze]
        
        # initialize table results
        self.results['tables'][table_name] = {
            'column_count': len(columns),
            'columns': {},
            'is_large_table': is_large_table
        }
        
        if optimization_config:
            self.results['tables'][table_name]['optimization_applied'] = {
                'sample_rate': optimization_config.get('sample_rate'),
                'time_range_days': optimization_config.get('time_range_days'),
                'key_columns_only': 'key_columns' in optimization_config
            }
        
        # analyze each column
        print(f"\nanalyze column statistics ({len(columns_to_analyze)} columns):")
        for column in columns_to_analyze:
            column_name = column['column_name']
            column_stats = {
                'data_type': column['data_type'],
                'is_nullable': column['is_nullable'],
                'column_key': column['column_key'],
                'statistics': self.analyze_column(table_name, column, optimization_config)
            }
            self.results['tables'][table_name]['columns'][column_name] = column_stats
        
    def analyze_all_tables(self):
        """analyze all core tables"""
        print(f"\nanalyze {len(self.core_tables)} core tables...")
        
        for table_name in self.core_tables:
            try:
                self.analyze_table(table_name)
            except Exception as e:
                print(f"✗ analyze table {table_name} failed: {e}, {traceback.format_exc()}")
                continue
        
    def save_results(self, output_file: str = 'production_data_profile.json'):
        """save analysis results to JSON file"""
        output_path = os.path.join(os.path.dirname(__file__), output_file)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False, cls=DecimalEncoder)
        print(f"\n✓ analysis results saved to: {output_path}")
    
    def generate_summary_report(self) -> str:
        """generate analysis summary report"""
        report = [
            "\n" + "="*60 + " data analysis summary report" + "="*60,
            f"\nanalysis time: {self.results['metadata']['analysis_date']}",
            f"database: {self.results['metadata']['database']}",
            f"\nanalyze {len(self.results['tables'])} tables\n"
        ]

        for table_name, table_data in self.results['tables'].items():
            report.append(f"\ntable: {table_name}")
            report.append(f"  - column count: {table_data['column_count']}")
        
        report.append("\n" + "="*60)
        return "\n".join(report)
    
    def run(self, output_file: str = 'production_data_profile.json'):
        """execute complete analysis process"""
        try:
            self.connect()
            self.analyze_all_tables()
            self.save_results(output_file)
            print(self.generate_summary_report())
        except Exception as e:
            print(f"\n✗ analyze process failed: {e}")
            raise
        finally:
            self.close()


def main():
    """main function"""
    # read configuration from environment variables
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '3306')),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'tenant'),
        # analyze options
        'max_columns_to_analyze': int(os.getenv('MAX_COLUMNS_TO_ANALYZE', '50')),
        # large table optimization options
        'activity_sample_rate': float(os.getenv('ACTIVITY_SAMPLE_RATE', '0.01')),  # 1%，sample rate
        'activity_time_range_days': int(os.getenv('ACTIVITY_TIME_RANGE_DAYS', '90'))  # 90 days，time range
    }
    
    print("analyze content includes:")
    print("  - table and column statistics")
    print("  - data distribution features (without actual values)")
    print("  - data quality metrics (missing rate, uniqueness, etc.)")
    print(f"\nconfiguration:")
    print(f"  - database: {config['database']}")
    print(f"  - host: {config['host']}:{config['port']}")
    print(f"  - max columns to analyze: {config['max_columns_to_analyze']}")
    print(f"\nlarge table optimization:")
    print(f"  - activity sample rate: {config['activity_sample_rate']*100:.1f}%")
    print(f"  - activity time range: {config['activity_time_range_days']} days")
    
    # create analyzer and run
    profiler = ProductionDataProfiler(config)
    profiler.run('production_data_profile.json')


if __name__ == '__main__':
    main()

