#!/usr/bin/env python3
"""
data relationship analysis
"""

import json
import os
from decimal import Decimal
from typing import Dict, List, Any
import pymysql
from pymysql.cursors import DictCursor


class DecimalEncoder(json.JSONEncoder):
    """custom json encoder"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


class DataRelationshipAnalyzer:
    """data relationship analyzer"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        initialize data relationship analyzer
        :param config: database connection configuration
        """
        self.config = config
        self.connection = None
        self.results = {
            'metadata': {
                'database': config.get('database', 'unknown')
            },
            'relationships': {},
            'data_flow_patterns': {}
        }
        
        # large table optimization configuration
        self.activity_time_range_days = config.get('activity_time_range_days', 90)  # default 90 days
    
    def connect(self):
        """connect to MySQL database"""
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
            print(f"✗ connect to database failed: {e}")
            raise
    
    def close(self):
        """close database connection"""
        if self.connection:
            self.connection.close()
    
    def execute_query(self, query: str) -> List[Dict]:
        """execute SQL query and return result"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            print(f"✗ execute query failed: {e}")
            return []
    
    def analyze_account_person_relationship(self):
        """analyze account_base and person_norm relationship"""
        print("\nanalyze account_base <-> person_norm relationship...")
        
        # 1. basic relationship statistics
        query = """
            WITH AccountPersonCounts AS (
                SELECT
                    account_id,
                    COUNT(*) AS person_count
                FROM person_norm
                GROUP BY account_id
            ),
            PersonStats AS (
                SELECT
                    AVG(person_count) AS avg_persons_per_account,
                    MIN(person_count) AS min_persons_per_account,
                    MAX(person_count) AS max_persons_per_account,
                    STDDEV(person_count) AS std_persons_per_account
                FROM AccountPersonCounts
            ),
            OverallStats AS (
                SELECT
                    (SELECT APPROX_COUNT_DISTINCT(id) FROM account_base) AS unique_accounts,
                    (SELECT APPROX_COUNT_DISTINCT(id) FROM person_norm) AS unique_persons,
                    APPROX_COUNT_DISTINCT(ab.id) AS unique_accounts_with_persons
                FROM account_base ab
                INNER JOIN AccountPersonCounts apc ON ab.id = apc.account_id
            )
            SELECT
                o.unique_accounts,
                o.unique_persons,
                o.unique_accounts_with_persons,
                p.avg_persons_per_account,
                p.min_persons_per_account,
                p.max_persons_per_account,
                p.std_persons_per_account
            FROM OverallStats o
            CROSS JOIN PersonStats p;
        """
        result = self.execute_query(query)
        
        if result:
            data = result[0]
            relationship = {
                'unique_accounts': data['unique_accounts'],
                'unique_persons': data['unique_persons'],
                'accounts_with_persons': data['unique_accounts_with_persons'],
                'accounts_without_persons': data['unique_accounts'] - data['unique_accounts_with_persons'],
                'avg_persons_per_account': round(float(data['avg_persons_per_account']), 2) if data['avg_persons_per_account'] else 0,
                'min_persons_per_account': data['min_persons_per_account'],
                'max_persons_per_account': data['max_persons_per_account'],
                'std_persons_per_account': round(float(data['std_persons_per_account']), 2) if data['std_persons_per_account'] else 0
            }
            
            # 2. number of persons distribution pattern，
            # count the number of persons distribution, the number of accounts with the same number of persons and the percentage
            dist_query = """
            SELECT 
                person_count,
                COUNT(*) as account_count,
                COUNT(*) * 100.0 / (SELECT APPROX_COUNT_DISTINCT(account_id) FROM person_norm) as percentage
            FROM (
                SELECT account_id, COUNT(*) as person_count
                FROM person_norm
                GROUP BY account_id
            ) person_dist
            GROUP BY person_count
            ORDER BY person_count
            LIMIT 100
            """
            distribution = self.execute_query(dist_query)
            relationship['person_count_distribution'] = [
                {
                    'person_count': d['person_count'],
                    'account_count': d['account_count'],
                    'percentage': round(d['percentage'], 2)
                }
                for d in distribution
            ]
            
            # 3. bucket statistics
            # count the number of persons distribution, the number of accounts with the same number of persons and the percentage
            bucket_query = """
            WITH 
                total_accounts AS (
                    SELECT COUNT(*) as total_cnt FROM account_base
                ),
                person_counts AS (
                    SELECT 
                        ab.id,
                        COUNT(pn.id) as person_count
                    FROM account_base ab
                    LEFT JOIN person_norm pn ON ab.id = pn.account_id
                    GROUP BY ab.id
                ),
                ranged_counts AS (
                    SELECT 
                        CASE 
                            WHEN person_count = 0 THEN 1
                            WHEN person_count <= 5 THEN 2
                            WHEN person_count <= 10 THEN 3
                            WHEN person_count <= 20 THEN 4
                            WHEN person_count <= 50 THEN 5
                            WHEN person_count <= 100 THEN 6
                            WHEN person_count <= 500 THEN 7
                            ELSE 8
                        END as range_id,
                        CASE 
                            WHEN person_count = 0 THEN '0'
                            WHEN person_count <= 5 THEN '1-5'
                            WHEN person_count <= 10 THEN '6-10'
                            WHEN person_count <= 20 THEN '11-20'
                            WHEN person_count <= 50 THEN '21-50'
                            WHEN person_count <= 100 THEN '51-100'
                            WHEN person_count <= 500 THEN '101-500'
                            ELSE '500+'
                        END as person_range
                    FROM person_counts
                )
                SELECT 
                    person_range,
                    COUNT(*) as account_count,
                    ROUND(COUNT(*) * 100.0 / (SELECT total_cnt FROM total_accounts), 2) as percentage
                FROM ranged_counts
                GROUP BY range_id, person_range
                ORDER BY range_id;
            """
            buckets = self.execute_query(bucket_query)
            relationship['person_count_buckets'] = [
                {
                    'range': b['person_range'],
                    'account_count': b['account_count'],
                    'percentage': round(b['percentage'], 2)
                }
                for b in buckets
            ]
            
            self.results['relationships']['account_person'] = relationship
            print(f"  ✓ found {relationship['unique_accounts']} accounts, {relationship['unique_persons']} persons")
    
    def analyze_account_activity_relationship(self):
        """analyze account_base and activity relationship"""
        print(f"\nanalyze account_base <-> activity relationship (last {self.activity_time_range_days} days)...")
        
        # 1. basic relationship statistics (optimized with time range)
        query = f"""
        WITH RecentActivities AS (
            SELECT
                id,
                account_id
            FROM activity
            WHERE activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
        ),
        AccountActivityCounts AS (
            SELECT
                account_id,
                COUNT(*) AS activity_count
            FROM RecentActivities
            GROUP BY account_id
        ),
        ActivityStats AS (
            SELECT
                AVG(activity_count) AS avg_activities_per_account,
                MIN(activity_count) AS min_activities_per_account,
                MAX(activity_count) AS max_activities_per_account,
                STDDEV(activity_count) AS std_activities_per_account
            FROM AccountActivityCounts
        ),
        OverallStats AS (
            SELECT
                (SELECT APPROX_COUNT_DISTINCT(id) FROM account_base) AS unique_accounts,
                (SELECT APPROX_COUNT_DISTINCT(id) FROM RecentActivities) AS unique_activities,
                APPROX_COUNT_DISTINCT(ab.id) AS unique_accounts_with_activities
            FROM account_base ab
            INNER JOIN AccountActivityCounts aac ON ab.id = aac.account_id
        )
        SELECT
            o.unique_accounts,
            o.unique_activities,
            o.unique_accounts_with_activities,
            s.avg_activities_per_account,
            s.min_activities_per_account,
            s.max_activities_per_account,
            s.std_activities_per_account
        FROM OverallStats o
        CROSS JOIN ActivityStats s;
        """        
        
        result = self.execute_query(query)
        
        if result:
            data = result[0]
            relationship = {
                'unique_accounts': data['unique_accounts'],
                'unique_activities': data['unique_activities'],
                'accounts_with_activities': data['unique_accounts_with_activities'],
                'accounts_without_activities': data['unique_accounts'] - data['unique_accounts_with_activities'],
                'avg_activities_per_account': round(float(data['avg_activities_per_account']), 2) if data['avg_activities_per_account'] else 0,
                'min_activities_per_account': data['min_activities_per_account'],
                'max_activities_per_account': data['max_activities_per_account'],
                'std_activities_per_account': round(float(data['std_activities_per_account']), 2) if data['std_activities_per_account'] else 0
            }
            
            # 2. activity count distribution (bucket statistics with time range optimization)
            # count the number of activities distribution, the number of accounts with the same number of activities and the percentage
            bucket_query = f"""
            WITH total_accounts AS (
                SELECT COUNT(*) as total_cnt FROM account_base
            ),
            account_activities AS (
                SELECT 
                    ab.id,
                    COUNT(a.id) as activity_count
                FROM account_base ab
                LEFT JOIN activity a ON ab.id = a.account_id 
                    AND a.activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
                GROUP BY ab.id
            ),
            activity_ranges AS (
                SELECT 
                    CASE 
                        WHEN activity_count = 0 THEN 1
                        WHEN activity_count <= 10 THEN 2
                        WHEN activity_count <= 50 THEN 3
                        WHEN activity_count <= 100 THEN 4
                        WHEN activity_count <= 500 THEN 5
                        WHEN activity_count <= 1000 THEN 6
                        WHEN activity_count <= 5000 THEN 7
                        ELSE 8
                    END as range_id,
                    CASE 
                        WHEN activity_count = 0 THEN '0'
                        WHEN activity_count <= 10 THEN '1-10'
                        WHEN activity_count <= 50 THEN '11-50'
                        WHEN activity_count <= 100 THEN '51-100'
                        WHEN activity_count <= 500 THEN '101-500'
                        WHEN activity_count <= 1000 THEN '501-1000'
                        WHEN activity_count <= 5000 THEN '1001-5000'
                        ELSE '5000+'
                    END as activity_range
                FROM account_activities
            )
            SELECT 
                activity_range,
                COUNT(*) as account_count,
                ROUND(COUNT(*) * 100.0 / (SELECT total_cnt FROM total_accounts), 2) as percentage
            FROM activity_ranges
            GROUP BY range_id, activity_range
            ORDER BY range_id;
            """
            buckets = self.execute_query(bucket_query)
            relationship['activity_count_buckets'] = [
                {
                    'range': b['activity_range'],
                    'account_count': b['account_count'],
                    'percentage': round(b['percentage'], 2)
                }
                for b in buckets
            ]
            
            # 3. activity type distribution (with time range optimization)
            type_query = f"""
            SELECT
                activityType,
                activity_count AS count,
                -- use window function to calculate total and percentage
                (activity_count * 100.0 / SUM(activity_count) OVER ()) AS percentage
            FROM (
                -- 1. filter data and calculate the number of each activityType
                SELECT
                    activityType,
                    COUNT(*) as activity_count
                FROM activity
                WHERE activityType IS NOT NULL
                AND activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
                GROUP BY activityType
            ) AS grouped_data
            ORDER BY count DESC
            LIMIT 20;
            """
            activity_types = self.execute_query(type_query)
            relationship['activity_type_distribution'] = [
                {
                    'type_category': 'type_' + str(i),  # do not expose actual type names
                    'count': at['count'],
                    'percentage': round(at['percentage'], 2)
                }
                for i, at in enumerate(activity_types)
            ]
            
            # add optimization metadata
            relationship['optimization_applied'] = {
                'time_range_days': self.activity_time_range_days,
                'note': f'Analysis based on last {self.activity_time_range_days} days of activity data'
            }
            
            self.results['relationships']['account_activity'] = relationship
            print(f"  ✓ found {relationship['unique_accounts']} accounts, {relationship['unique_activities']} activities")
    
    def analyze_person_activity_relationship(self):
        """analyze person_norm and activity relationship"""
        print(f"\nanalyze person_norm <-> activity relationship (last {self.activity_time_range_days} days)...")
        
        # 1. basic relationship statistics (optimized with time range)
        query = f"""
        WITH activity_counts AS (
            SELECT 
                person_id, 
                COUNT(*) as activity_count
            FROM activity 
            WHERE activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
            GROUP BY person_id
        )
        SELECT 
            APPROX_COUNT_DISTINCT(pn.id) as unique_persons,
            APPROX_COUNT_DISTINCT(a.id) as unique_activities,
            APPROX_COUNT_DISTINCT(CASE WHEN ac.activity_count > 0 THEN pn.id END) as unique_persons_with_activities,
            AVG(COALESCE(ac.activity_count, 0)) as avg_activities_per_person,
            MIN(COALESCE(ac.activity_count, 0)) as min_activities_per_person,
            MAX(COALESCE(ac.activity_count, 0)) as max_activities_per_person,
            STDDEV(COALESCE(ac.activity_count, 0)) as std_activities_per_person
        FROM person_norm pn
        LEFT JOIN activity_counts ac ON pn.id = ac.person_id
        LEFT JOIN activity a ON pn.id = a.person_id 
            AND a.activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
        """
        result = self.execute_query(query)
        
        if result:
            data = result[0]
            relationship = {
                'unique_persons': data['unique_persons'],
                'unique_activities': data['unique_activities'],
                'persons_with_activities': data['unique_persons_with_activities'],
                'persons_without_activities': data['unique_persons'] - data['unique_persons_with_activities'],
                'avg_activities_per_person': round(float(data['avg_activities_per_person']), 2) if data['avg_activities_per_person'] else 0,
                'min_activities_per_person': data['min_activities_per_person'],
                'max_activities_per_person': data['max_activities_per_person'],
                'std_activities_per_person': round(float(data['std_activities_per_person']), 2) if data['std_activities_per_person'] else 0
            }
            
            # 2. activity count distribution (bucket statistics with time range optimization)
            bucket_query = f"""
            SELECT 
                CASE 
                    WHEN activity_count = 0 THEN '0'
                    WHEN activity_count BETWEEN 1 AND 10 THEN '1-10'
                    WHEN activity_count BETWEEN 11 AND 50 THEN '11-50'
                    WHEN activity_count BETWEEN 51 AND 100 THEN '51-100'
                    WHEN activity_count BETWEEN 101 AND 500 THEN '101-500'
                    WHEN activity_count BETWEEN 501 AND 1000 THEN '501-1000'
                    ELSE '1000+'
                END as activity_range,
                COUNT(*) as person_count,
                COUNT(*) * 100.0 / (SELECT APPROX_COUNT_DISTINCT(id) FROM person_norm) as percentage
            FROM (
                SELECT pn.id, COALESCE(COUNT(a.id), 0) as activity_count
                FROM person_norm pn
                LEFT JOIN activity a ON pn.id = a.person_id 
                    AND a.activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
                GROUP BY pn.id
            ) activity_dist
            GROUP BY activity_range
            ORDER BY 
                CASE activity_range
                    WHEN '0' THEN 1
                    WHEN '1-10' THEN 2
                    WHEN '11-50' THEN 3
                    WHEN '51-100' THEN 4
                    WHEN '101-500' THEN 5
                    WHEN '501-1000' THEN 6
                    ELSE 7
                END
            """
            buckets = self.execute_query(bucket_query)
            relationship['activity_count_buckets'] = [
                {
                    'range': b['activity_range'],
                    'person_count': b['person_count'],
                    'percentage': round(b['percentage'], 2)
                }
                for b in buckets
            ]
            
            # add optimization metadata
            relationship['optimization_applied'] = {
                'time_range_days': self.activity_time_range_days,
                'note': f'Analysis based on last {self.activity_time_range_days} days of activity data'
            }
            
            self.results['relationships']['person_activity'] = relationship
            print(f"  ✓ found {relationship['unique_persons']} persons, {relationship['unique_activities']} activities")
    
    def analyze_account_list_patterns(self):
        """analyze account_list_member patterns"""
        print("\nanalyze account_list_member patterns...")
        
        # 1. basic list statistics
        query = """
            WITH ListSizes AS (
                SELECT
                    account_list_id,
                    COUNT(*) AS member_count
                FROM account_list_member
                GROUP BY account_list_id
            ),
            ListStats AS (
                SELECT
                    AVG(member_count) AS avg_members_per_list,
                    MIN(member_count) AS min_members_per_list,
                    MAX(member_count) AS max_members_per_list,
                    STDDEV(member_count) AS std_members_per_list
                FROM ListSizes
            ),
            OverallStats AS (
                SELECT
                    APPROX_COUNT_DISTINCT(account_list_id) AS unique_lists,
                    APPROX_COUNT_DISTINCT(account_id) AS unique_accounts,
                    COUNT(*) AS total_memberships
                FROM account_list_member
            )
            SELECT
                o.unique_lists,
                o.unique_accounts,
                o.total_memberships,
                s.avg_members_per_list,
                s.min_members_per_list,
                s.max_members_per_list,
                s.std_members_per_list
            FROM OverallStats o
            CROSS JOIN ListStats s;
        """
        result = self.execute_query(query)
        
        if result:
            data = result[0]
            pattern = {
                'unique_lists': data['unique_lists'],
                'unique_accounts': data['unique_accounts'],
                'total_memberships': data['total_memberships'],
                'avg_members_per_list': round(float(data['avg_members_per_list']), 2) if data['avg_members_per_list'] else 0,
                'min_members_per_list': data['min_members_per_list'],
                'max_members_per_list': data['max_members_per_list'],
                'std_members_per_list': round(float(data['std_members_per_list']), 2) if data['std_members_per_list'] else 0
            }
            
            # 2. list size distribution
            bucket_query = """
            SELECT 
                CASE 
                    WHEN member_count BETWEEN 1 AND 10 THEN '1-10'
                    WHEN member_count BETWEEN 11 AND 50 THEN '11-50'
                    WHEN member_count BETWEEN 51 AND 100 THEN '51-100'
                    WHEN member_count BETWEEN 101 AND 500 THEN '101-500'
                    WHEN member_count BETWEEN 501 AND 1000 THEN '501-1000'
                    WHEN member_count BETWEEN 1001 AND 5000 THEN '1001-5000'
                    ELSE '5000+'
                END as size_range,
                COUNT(*) as list_count,
                COUNT(*) * 100.0 / (SELECT APPROX_COUNT_DISTINCT(account_list_id) FROM account_list_member) as percentage
            FROM (
                SELECT account_list_id, COUNT(*) as member_count
                FROM account_list_member
                GROUP BY account_list_id
            ) list_sizes
            GROUP BY size_range
            ORDER BY 
                CASE size_range
                    WHEN '1-10' THEN 1
                    WHEN '11-50' THEN 2
                    WHEN '51-100' THEN 3
                    WHEN '101-500' THEN 4
                    WHEN '501-1000' THEN 5
                    WHEN '1001-5000' THEN 6
                    ELSE 7
                END
            """
            buckets = self.execute_query(bucket_query)
            pattern['list_size_buckets'] = [
                {
                    'range': b['size_range'],
                    'list_count': b['list_count'],
                    'percentage': round(b['percentage'], 2)
                }
                for b in buckets
            ]
            
            # 3. number of lists per account
            membership_query = """
            SELECT 
                AVG(list_count) as avg_lists_per_account,
                MIN(list_count) as min_lists_per_account,
                MAX(list_count) as max_lists_per_account,
                STDDEV(list_count) as std_lists_per_account
            FROM (
                SELECT account_id, COUNT(*) as list_count
                FROM account_list_member
                GROUP BY account_id
            ) account_memberships
            """
            membership_result = self.execute_query(membership_query)
            if membership_result:
                m_data = membership_result[0]
                pattern['avg_lists_per_account'] = round(float(m_data['avg_lists_per_account']), 2) if m_data['avg_lists_per_account'] else 0
                pattern['min_lists_per_account'] = m_data['min_lists_per_account']
                pattern['max_lists_per_account'] = m_data['max_lists_per_account']
                pattern['std_lists_per_account'] = round(float(m_data['std_lists_per_account']), 2) if m_data['std_lists_per_account'] else 0
            
            self.results['relationships']['account_list_member'] = pattern
            print(f"  ✓ found {pattern['unique_lists']} lists, {pattern['unique_accounts']} accounts")

    
    def analyze_temporal_patterns(self):
        """analyze temporal patterns"""
        print("\nanalyze temporal patterns...")
        
        temporal = {}
        
        # 1. activity temporal patterns
        activity_temporal_query = f"""
        SELECT 
            YEAR(activity_date) as year,
            MONTH(activity_date) as month,
            COUNT(*) as count,
            APPROX_COUNT_DISTINCT(account_id) as unique_accounts,
            APPROX_COUNT_DISTINCT(person_id) as unique_persons
        FROM activity
        WHERE activity_date IS NOT NULL AND activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
        GROUP BY YEAR(activity_date), MONTH(activity_date)
        ORDER BY year DESC, month DESC
        LIMIT 24
        """
        activity_temporal = self.execute_query(activity_temporal_query)
        temporal['activity_monthly'] = [
            {
                'year': t['year'],
                'month': t['month'],
                'count': t['count'],
                'unique_accounts': t['unique_accounts'],
                'unique_persons': t['unique_persons']
            }
            for t in activity_temporal
        ]

        self.results['data_flow_patterns']['temporal'] = temporal
        print("  ✓ analyze temporal patterns completed")
    
    def save_results(self, output_file: str = 'data_relationship_analysis.json'):
        """save analysis results"""
        output_path = os.path.join(os.path.dirname(__file__), output_file)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False, cls=DecimalEncoder)
        print(f"\n✓ analysis results saved to: {output_path}")
    
    def run(self, output_file: str = 'data_relationship_analysis.json'):
        """execute full data relationship analysis"""
        try:
            self.connect()
            
            print("\n" + "="*60 + " data relationship analysis " + "="*60)
            
            self.analyze_account_person_relationship()
            self.analyze_account_activity_relationship()
            self.analyze_person_activity_relationship()
            self.analyze_account_list_patterns()
            self.analyze_temporal_patterns()
            
            self.save_results(output_file)
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
        'port': int(os.getenv('DB_PORT', 3306)),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'tenant'),
        # optimization options
        'activity_time_range_days': int(os.getenv('ACTIVITY_TIME_RANGE_DAYS', '90'))  # 90 days
    }
    
    print("analyze content includes:")
    print("  - cardinality between tables (1-to-many, many-to-many, etc.)")
    print("  - data distribution patterns")
    print("  - temporal patterns")
    print("  - data flow analysis")
    print(f"\noptimization configuration:")
    print(f"  - activity time range: {config['activity_time_range_days']} days")
    
    # create analyzer and run
    analyzer = DataRelationshipAnalyzer(config)
    analyzer.run('data_relationship_analysis.json')


if __name__ == '__main__':
    main()