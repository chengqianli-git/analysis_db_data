#!/usr/bin/env python3
"""
Sample Account Analyzer - analyze the relationship of specified accounts
"""

import json
import os
from decimal import Decimal
import traceback
from typing import Dict, List, Any
import pymysql
from pymysql.cursors import DictCursor


class DecimalEncoder(json.JSONEncoder):
    """custom json encoder"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


class SampleAccountAnalyzer:
    """sample account relationship analyzer"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        initialize analyzer
        :param config: database connection configuration
        """
        self.config = config
        self.connection = None
        self.sample_size = config.get('sample_size', 50)  # default 50 accounts
        self.activity_time_range_days = config.get('activity_time_range_days', 90)
        self.activity_sample_rate = config.get('activity_sample_rate', 0.01)  # default 1% sampling for activity queries
        self.account_ids = config.get('account_ids', [])  # optional: specify specific account IDs, leave blank for random sampling
        
        self.results = {
            'metadata': {
                'database': config.get('database', 'unknown'),
                'sample_size': self.sample_size,
                'activity_time_range_days': self.activity_time_range_days,
                'activity_sample_rate': self.activity_sample_rate,
                'sampling_method': 'specified_ids' if self.account_ids else 'random_sample'
            },
            'sample_account_stats': {},
            'aggregated_stats': {}
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
            print(f"✗ connect to database failed: {e}")
            raise
    
    def close(self):
        """close database connection"""
        if self.connection:
            self.connection.close()
    
    def execute_query(self, query: str) -> List[Dict]:
        """execute SQL query"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            print(f"✗ execute query failed: {e}")
            print(f"SQL: {query[:200]}...")
            return []
    
    def get_sample_account_ids(self) -> List[int]:
        """
        get sample account IDs
        method 1: if specified, use specified account IDs, ignore sample size
        method 2: if not specified, random sample
        """
        if self.account_ids:
            print(f"use specified {len(self.account_ids)} account IDs")
            return self.account_ids
        
        print(f"random sample {self.sample_size} accounts from account_base table")
        
        # 先估算总行数，计算采样概率
        count_query = "SELECT COUNT(*) as total FROM account_base"
        count_result = self.execute_query(count_query)
        total_accounts = count_result[0]['total'] if count_result else 0
        
        if total_accounts == 0:
            print("⚠️  account_base表为空")
            return []
        
        # 计算采样概率（略高于目标，确保足够样本）
        sample_probability = self.sample_size / total_accounts
        
        print(f"  ✓ 总账户数: {total_accounts:,}, 采样概率: {sample_probability:.6f}")
        
        query = f"""
        SELECT id 
        FROM account_base 
        WHERE RAND() < {sample_probability}
        LIMIT {self.sample_size}
        """
        result = self.execute_query(query)
        account_ids = [row['id'] for row in result]
        
        print(f"✓ get {len(account_ids)} sample account IDs")
        return account_ids
    
    def analyze_account_person_counts(self, account_ids: List[int]) -> Dict:
        """
        analyze the number of persons in specified accounts
        """
        print(f"\nanalyze {len(account_ids)} accounts' person relationship...")
        
        ids_str = ','.join(str(id) for id in account_ids)
        
        query = f"""
        SELECT 
            account_id,
            COUNT(*) as person_count
        FROM person_norm
        WHERE account_id IN ({ids_str})
        GROUP BY account_id
        """
        
        result = self.execute_query(query)
        
        # build statistics for each account
        account_person_map = {row['account_id']: row['person_count'] for row in result}
        
        # accounts with 0 persons
        person_counts = []
        for aid in account_ids:
            count = account_person_map.get(aid, 0)
            person_counts.append(count)
        
        # calculate aggregated statistics
        stats = {
            'sample_size': len(account_ids),
            'accounts_with_persons': len([c for c in person_counts if c > 0]),
            'accounts_without_persons': len([c for c in person_counts if c == 0]),
            'total_persons': sum(person_counts),
            'avg_persons_per_account': round(sum(person_counts) / len(person_counts), 2) if person_counts else 0,
            'min_persons_per_account': min(person_counts) if person_counts else 0,
            'max_persons_per_account': max(person_counts) if person_counts else 0,
            'std_persons_per_account': round(self._calculate_std(person_counts), 2) if person_counts else 0
        }
        
        # bucket statistics
        buckets = self._create_buckets(
            person_counts,
            [(0, 0), (1, 5), (6, 10), (11, 20), (21, 50), (51, 100), (101, 500), (501, float('inf'))],
            ['0', '1-5', '6-10', '11-20', '21-50', '51-100', '101-500', '500+']
        )
        stats['person_count_buckets'] = buckets
        
        print(f"  ✓ average {stats['avg_persons_per_account']} persons per account")
        print(f"  ✓ {stats['accounts_without_persons']} accounts without persons")
        
        return stats
    
    def analyze_account_activity_counts(self, account_ids: List[int]) -> Dict:
        """
        analyze the number of activities in specified accounts
        """
        print(f"\nanalyze {len(account_ids)} accounts' activity relationship (last {self.activity_time_range_days} days, {self.activity_sample_rate*100:.1f}% sample)...")
        
        ids_str = ','.join(str(id) for id in account_ids)
        
        query = f"""
        SELECT 
            account_id,
            COUNT(*) * {1.0/self.activity_sample_rate} as activity_count
        FROM activity
        WHERE account_id IN ({ids_str})
          AND activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
          AND RAND() < {self.activity_sample_rate}
        GROUP BY account_id
        """
        
        result = self.execute_query(query)
        
        # build statistics for each account (convert Decimal to int)
        account_activity_map = {row['account_id']: int(row['activity_count']) for row in result}
        
        # accounts with 0 activities
        activity_counts = []
        for aid in account_ids:
            count = account_activity_map.get(aid, 0)
            activity_counts.append(count)
        
        # calculate aggregated statistics
        stats = {
            'sample_size': len(account_ids),
            'accounts_with_activities': len([c for c in activity_counts if c > 0]),
            'accounts_without_activities': len([c for c in activity_counts if c == 0]),
            'total_activities': sum(activity_counts),
            'avg_activities_per_account': round(sum(activity_counts) / len(activity_counts), 2) if activity_counts else 0,
            'min_activities_per_account': min(activity_counts) if activity_counts else 0,
            'max_activities_per_account': max(activity_counts) if activity_counts else 0,
            'std_activities_per_account': round(self._calculate_std(activity_counts), 2) if activity_counts else 0,
            'time_range_days': self.activity_time_range_days
        }
        
        # bucket statistics
        buckets = self._create_buckets(
            activity_counts,
            [(0, 0), (1, 10), (11, 50), (51, 100), (101, 500), (501, 1000), (1001, 5000), (5001, float('inf'))],
            ['0', '1-10', '11-50', '51-100', '101-500', '501-1000', '1001-5000', '5000+']
        )
        stats['activity_count_buckets'] = buckets
        
        print(f"  ✓ average {stats['avg_activities_per_account']} activities per account")
        print(f"  ✓ {stats['accounts_without_activities']} accounts without activities")
        
        return stats
    
    def analyze_person_activity_counts(self, account_ids: List[int]) -> Dict:
        """
        analyze the number of activities in specified accounts
        """
        print(f"\nanalyze {len(account_ids)} accounts' person activity relationship (last {self.activity_time_range_days} days, {self.activity_sample_rate*100:.1f}% sample)...")
        
        ids_str = ','.join(str(id) for id in account_ids)
        
        # get all person_ids in the specified accounts
        person_query = f"""
        SELECT id as person_id
        FROM person_norm
        WHERE account_id IN ({ids_str})
        """
        person_result = self.execute_query(person_query)
        person_ids = [row['person_id'] for row in person_result]
        
        if not person_ids:
            print("  ⚠️  no persons in the sample accounts")
            return {}
        
        print(f"  ✓ found {len(person_ids)} persons")
        
        # query the number of activities for these persons (with sampling)
        person_ids_str = ','.join(str(id) for id in person_ids)
        
        query = f"""
        SELECT 
            person_id,
            COUNT(*) * {1.0/self.activity_sample_rate} as activity_count
        FROM activity
        WHERE person_id IN ({person_ids_str})
          AND activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
          AND RAND() < {self.activity_sample_rate}
        GROUP BY person_id
        """
        
        result = self.execute_query(query)
        
        person_activity_map = {row['person_id']: int(row['activity_count']) for row in result}
        
        activity_counts = []
        for pid in person_ids:
            count = person_activity_map.get(pid, 0)
            activity_counts.append(count)
        
        # calculate aggregated statistics
        stats = {
            'sample_persons': len(person_ids),
            'persons_with_activities': len([c for c in activity_counts if c > 0]),
            'persons_without_activities': len([c for c in activity_counts if c == 0]),
            'total_activities': sum(activity_counts),
            'avg_activities_per_person': round(sum(activity_counts) / len(activity_counts), 2) if activity_counts else 0,
            'min_activities_per_person': min(activity_counts) if activity_counts else 0,
            'max_activities_per_person': max(activity_counts) if activity_counts else 0,
            'std_activities_per_person': round(self._calculate_std(activity_counts), 2) if activity_counts else 0,
            'time_range_days': self.activity_time_range_days
        }
        
        # bucket statistics
        buckets = self._create_buckets(
            activity_counts,
            [(0, 0), (1, 10), (11, 50), (51, 100), (101, 500), (501, 1000), (1001, float('inf'))],
            ['0', '1-10', '11-50', '51-100', '101-500', '501-1000', '1000+']
        )
        stats['activity_count_buckets'] = buckets
        
        print(f"  ✓ average {stats['avg_activities_per_person']} activities per person")
        
        return stats
    
    def analyze_activity_types(self, account_ids: List[int]) -> Dict:
        """
        analyze the distribution of activity types in specified accounts
        """
        print(f"\nanalyze {len(account_ids)} accounts' activity type distribution ({self.activity_sample_rate*100:.1f}% sample)...")
        
        ids_str = ','.join(str(id) for id in account_ids)
        
        query = f"""
        SELECT 
            activityType,
            COUNT(*) as count
        FROM activity
        WHERE account_id IN ({ids_str})
          AND activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
          AND activityType IS NOT NULL
          AND RAND() < {self.activity_sample_rate}
        GROUP BY activityType
        ORDER BY count DESC
        LIMIT 20
        """
        
        result = self.execute_query(query)
        
        # 计算百分比
        total_count = sum(row['count'] for row in result)
        
        activity_types = [
            {
                'type_category': 'type_' + str(i),  # do not expose actual type names
                'count': row['count'],
                'percentage': round(row['count'] * 100.0 / total_count, 2) if total_count > 0 else 0
            }
            for i, row in enumerate(result)
        ]
        
        print(f"  ✓ found {len(activity_types)} activity types")
        
        return {
            'activity_type_distribution': activity_types,
            'total_types': len(activity_types)
        }
    
    def analyze_list_membership(self, account_ids: List[int]) -> Dict:
        """
        analyze the list membership relationship in specified accounts
        """
        print(f"\nanalyze {len(account_ids)} accounts' list membership relationship...")
        
        ids_str = ','.join(str(id) for id in account_ids)
        
        query = f"""
        SELECT 
            account_id,
            COUNT(*) as list_count
        FROM account_list_member
        WHERE account_id IN ({ids_str})
        GROUP BY account_id
        """
        
        result = self.execute_query(query)
        
        # build statistics for each account
        account_list_map = {row['account_id']: row['list_count'] for row in result}
        
        list_counts = []
        for aid in account_ids:
            count = account_list_map.get(aid, 0)
            list_counts.append(count)
        
        stats = {
            'sample_size': len(account_ids),
            'accounts_in_lists': len([c for c in list_counts if c > 0]),
            'accounts_not_in_lists': len([c for c in list_counts if c == 0]),
            'total_memberships': sum(list_counts),
            'avg_lists_per_account': round(sum(list_counts) / len(list_counts), 2) if list_counts else 0,
            'min_lists_per_account': min(list_counts) if list_counts else 0,
            'max_lists_per_account': max(list_counts) if list_counts else 0,
            'std_lists_per_account': round(self._calculate_std(list_counts), 2) if list_counts else 0
        }
        
        print(f"  ✓ average {stats['avg_lists_per_account']} lists per account")
        print(f"  ✓ {stats['accounts_not_in_lists']} accounts not in any lists")
        
        return stats
    
    def _calculate_std(self, values: List[float]) -> float:
        """calculate standard deviation"""
        if not values or len(values) < 2:
            return 0.0
        
        values = [float(v) for v in values]
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5
    
    def _create_buckets(self, values: List[int], ranges: List[tuple], labels: List[str]) -> List[Dict]:
        """
        create bucket statistics
        :param values: value list
        :param ranges: range list [(min, max), ...]
        :param labels: label list
        """
        buckets = {label: 0 for label in labels}
        
        for value in values:
            for (min_val, max_val), label in zip(ranges, labels):
                if min_val <= value <= max_val:
                    buckets[label] += 1
                    break
        
        total = len(values)
        return [
            {
                'range': label,
                'count': count,
                'percentage': round(count * 100.0 / total, 2) if total > 0 else 0
            }
            for label, count in [(l, buckets[l]) for l in labels]
        ]
    
    def run(self, output_file: str = 'sample_account_analysis.json'):
        """execute sample analysis"""
        try:
            self.connect()
            
            print(f"\nconfiguration:")
            print(f"  - sample size: {self.sample_size} accounts")
            print(f"  - activity time range: last {self.activity_time_range_days} days")
            print(f"  - sampling method: {'specified ID' if self.account_ids else 'random sampling'}")
            
            # step 1: get sample account IDs
            account_ids = self.get_sample_account_ids()
            self.results['metadata']['actual_sample_size'] = len(account_ids)
            self.results['metadata']['account_ids_sample'] = account_ids
            
            # step 2: analyze account-person relationship
            person_stats = self.analyze_account_person_counts(account_ids)
            self.results['aggregated_stats']['account_person'] = person_stats
            
            # step 3: analyze account-activity relationship
            activity_stats = self.analyze_account_activity_counts(account_ids)
            self.results['aggregated_stats']['account_activity'] = activity_stats
            
            # step 4: analyze person-activity relationship
            person_activity_stats = self.analyze_person_activity_counts(account_ids)
            self.results['aggregated_stats']['person_activity'] = person_activity_stats
            
            # step 5: analyze list membership relationship
            list_stats = self.analyze_list_membership(account_ids)
            self.results['aggregated_stats']['account_list'] = list_stats
            
            # step 6: analyze activity type distribution
            type_stats = self.analyze_activity_types(account_ids)
            self.results['aggregated_stats']['activity_types'] = type_stats
            
            # save results
            self.save_results(output_file)
            
            # print summary
            self.print_summary()
            
        except Exception as e:
            print(f"\n✗ analyze process failed: {e}, {traceback.format_exc()}")
            raise
        finally:
            self.close()
    
    def save_results(self, output_file: str):
        """save analysis results"""
        output_path = os.path.join(os.path.dirname(__file__), output_file)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False, cls=DecimalEncoder)
        print(f"\n✓ analysis results saved to: {output_path}")
    
    def print_summary(self):
        """print analysis summary"""
        print("\n" + "="*80)
        print("analysis summary")
        print("="*80)
        
        stats = self.results['aggregated_stats']
        
        if 'account_person' in stats:
            print(f"\naccount-person relationship:")
            print(f"  - sample account size: {stats['account_person']['sample_size']}")
            print(f"  - average persons per account: {stats['account_person']['avg_persons_per_account']}")
            print(f"  - standard deviation: {stats['account_person']['std_persons_per_account']}")
            print(f"  - person count range: {stats['account_person']['min_persons_per_account']} - {stats['account_person']['max_persons_per_account']}")
        
        if 'account_activity' in stats:
            print(f"\naccount-activity relationship (last {self.activity_time_range_days} days):")
            print(f"  - sample account size: {stats['account_activity']['sample_size']}")
            print(f"  - activities per account: {stats['account_activity']['avg_activities_per_account']}")
            print(f"  - standard deviation: {stats['account_activity']['std_activities_per_account']}")
            print(f"  - activity count range: {stats['account_activity']['min_activities_per_account']} - {stats['account_activity']['max_activities_per_account']}")
        
        if 'person_activity' in stats:
            print(f"\nperson-activity relationship (last {self.activity_time_range_days} days):")
            print(f"  - sample person size: {stats['person_activity']['sample_persons']}")
            print(f"  - average activities per person: {stats['person_activity']['avg_activities_per_person']}")
            print(f"  - standard deviation: {stats['person_activity']['std_activities_per_person']}")
            print(f"  - activity count range: {stats['person_activity']['min_activities_per_person']} - {stats['person_activity']['max_activities_per_person']}")
        
        if 'account_list' in stats:
            print(f"\naccount-list membership relationship:")
            print(f"  - sample account size: {stats['account_list']['sample_size']}")
            print(f"  - average lists per account: {stats['account_list']['avg_lists_per_account']}")
            print(f"  - accounts not in lists: {stats['account_list']['accounts_not_in_lists']} accounts")
            print(f"  - list count range: {stats['account_list']['min_lists_per_account']} - {stats['account_list']['max_lists_per_account']}")

        if 'activity_types' in stats:
            print(f"\nactivity type distribution:")
            print(f"  - total types: {stats['activity_types']['total_types']}")
            for type in stats['activity_types']['activity_type_distribution']:
                print(f"  - {type['type_category']}: {type['count']} ({type['percentage']}%)")
        
        print("\n" + "="*80)


def main():
    """main function"""
    # read account IDs from environment variable (if any)
    account_ids_str = os.getenv('ACCOUNT_IDS', '')
    account_ids = []
    if account_ids_str:
        account_ids = [int(aid.strip()) for aid in account_ids_str.split(',') if aid.strip()]
        print(f"read {len(account_ids)} account IDs from environment variable")
    
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '3306')),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'tenant'),
        'sample_size': int(os.getenv('SAMPLE_SIZE', '1000')),  # default 1000 accounts
        'activity_time_range_days': int(os.getenv('ACTIVITY_TIME_RANGE_DAYS', '90')),
        'activity_sample_rate': float(os.getenv('ACTIVITY_SAMPLE_RATE', '0.01')),  # default 1% sampling for activity queries
        'account_ids': account_ids  # optional: specify specific account IDs, leave blank for random sampling
    }
    
    print("\nfeatures:")
    print("  - can random sample or specify account IDs")
    
    analyzer = SampleAccountAnalyzer(config)
    analyzer.run('sample_account_analysis.json')


if __name__ == '__main__':
    main()

