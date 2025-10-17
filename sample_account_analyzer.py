#!/usr/bin/env python3
"""
Sample Account Analyzer - 分析指定账户的关联关系
"""

import json
import os
from decimal import Decimal
from typing import Dict, List, Any
import pymysql
from pymysql.cursors import DictCursor
import random


class DecimalEncoder(json.JSONEncoder):
    """custom json encoder"""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


class SampleAccountAnalyzer:
    """基于样本账户的关系分析器"""
    
    def __init__(self, config: Dict[str, Any]):
        """
        初始化分析器
        :param config: 数据库连接配置
        """
        self.config = config
        self.connection = None
        self.sample_size = config.get('sample_size', 50)  # 默认50个样本
        self.activity_time_range_days = config.get('activity_time_range_days', 90)
        self.account_ids = config.get('account_ids', [])  # 可以指定具体的account_id列表
        
        self.results = {
            'metadata': {
                'database': config.get('database', 'unknown'),
                'sample_size': self.sample_size,
                'activity_time_range_days': self.activity_time_range_days,
                'sampling_method': 'specified_ids' if self.account_ids else 'random_sample'
            },
            'sample_account_stats': {},
            'aggregated_stats': {}
        }
    
    def connect(self):
        """连接数据库"""
        try:
            self.connection = pymysql.connect(
                host=self.config['host'],
                port=self.config.get('port', 3306),
                user=self.config['user'],
                password=self.config['password'],
                database=self.config['database'],
                cursorclass=DictCursor
            )
            print(f"✓ 已连接到数据库: {self.config['database']}")
        except Exception as e:
            print(f"✗ 数据库连接失败: {e}")
            raise
    
    def close(self):
        """关闭数据库连接"""
        if self.connection:
            self.connection.close()
    
    def execute_query(self, query: str) -> List[Dict]:
        """执行SQL查询"""
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query)
                return cursor.fetchall()
        except Exception as e:
            print(f"✗ 查询执行失败: {e}")
            print(f"SQL: {query[:200]}...")
            return []
    
    def get_sample_account_ids(self) -> List[int]:
        """
        获取样本账户ID
        方法1：如果指定了account_ids，使用指定的
        方法2：如果没有指定，随机采样
        """
        if self.account_ids:
            print(f"使用指定的 {len(self.account_ids)} 个账户ID")
            return self.account_ids
        
        # 随机采样账户
        print(f"从account_base表中随机采样 {self.sample_size} 个账户...")
        
        query = f"""
        SELECT id
        FROM account_base
        ORDER BY RAND()
        LIMIT {self.sample_size}
        """
        
        result = self.execute_query(query)
        account_ids = [row['id'] for row in result]
        
        print(f"✓ 已获取 {len(account_ids)} 个样本账户ID")
        return account_ids
    
    def analyze_account_person_counts(self, account_ids: List[int]) -> Dict:
        """
        分析指定账户的人员数量
        使用 IN 子句，避免大表JOIN
        """
        print(f"\n分析 {len(account_ids)} 个账户的人员关系...")
        
        # 将account_ids转换为SQL IN子句
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
        
        # 构建每个账户的统计
        account_person_map = {row['account_id']: row['person_count'] for row in result}
        
        # 包含0个人员的账户
        person_counts = []
        for aid in account_ids:
            count = account_person_map.get(aid, 0)
            person_counts.append(count)
        
        # 计算聚合统计
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
        
        # 分桶统计
        buckets = self._create_buckets(
            person_counts,
            [(0, 0), (1, 5), (6, 10), (11, 20), (21, 50), (51, 100), (101, 500), (501, float('inf'))],
            ['0', '1-5', '6-10', '11-20', '21-50', '51-100', '101-500', '500+']
        )
        stats['person_count_buckets'] = buckets
        
        print(f"  ✓ 平均每个账户 {stats['avg_persons_per_account']} 个人员")
        print(f"  ✓ {stats['accounts_without_persons']} 个账户没有人员")
        
        return stats
    
    def analyze_account_activity_counts(self, account_ids: List[int]) -> Dict:
        """
        分析指定账户的活动数量（最近90天）
        使用 IN 子句，避免大表JOIN
        """
        print(f"\n分析 {len(account_ids)} 个账户的活动关系（最近{self.activity_time_range_days}天）...")
        
        ids_str = ','.join(str(id) for id in account_ids)
        
        query = f"""
        SELECT 
            account_id,
            COUNT(*) as activity_count
        FROM activity
        WHERE account_id IN ({ids_str})
          AND activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
        GROUP BY account_id
        """
        
        result = self.execute_query(query)
        
        # 构建每个账户的统计
        account_activity_map = {row['account_id']: row['activity_count'] for row in result}
        
        # 包含0个活动的账户
        activity_counts = []
        for aid in account_ids:
            count = account_activity_map.get(aid, 0)
            activity_counts.append(count)
        
        # 计算聚合统计
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
        
        # 分桶统计
        buckets = self._create_buckets(
            activity_counts,
            [(0, 0), (1, 10), (11, 50), (51, 100), (101, 500), (501, 1000), (1001, 5000), (5001, float('inf'))],
            ['0', '1-10', '11-50', '51-100', '101-500', '501-1000', '1001-5000', '5000+']
        )
        stats['activity_count_buckets'] = buckets
        
        print(f"  ✓ 平均每个账户 {stats['avg_activities_per_account']} 个活动")
        print(f"  ✓ {stats['accounts_without_activities']} 个账户没有活动")
        
        return stats
    
    def analyze_person_activity_counts(self, account_ids: List[int]) -> Dict:
        """
        分析指定账户下人员的活动数量
        """
        print(f"\n分析样本账户下人员的活动关系（最近{self.activity_time_range_days}天）...")
        
        ids_str = ','.join(str(id) for id in account_ids)
        
        # 先获取这些账户下的所有person_id
        person_query = f"""
        SELECT id as person_id
        FROM person_norm
        WHERE account_id IN ({ids_str})
        """
        person_result = self.execute_query(person_query)
        person_ids = [row['person_id'] for row in person_result]
        
        if not person_ids:
            print("  ⚠️  样本账户下没有人员")
            return {}
        
        print(f"  找到 {len(person_ids)} 个人员")
        
        # 查询这些人员的活动数量
        person_ids_str = ','.join(str(id) for id in person_ids)
        
        query = f"""
        SELECT 
            person_id,
            COUNT(*) as activity_count
        FROM activity
        WHERE person_id IN ({person_ids_str})
          AND activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
        GROUP BY person_id
        """
        
        result = self.execute_query(query)
        
        # 构建统计
        person_activity_map = {row['person_id']: row['activity_count'] for row in result}
        
        activity_counts = []
        for pid in person_ids:
            count = person_activity_map.get(pid, 0)
            activity_counts.append(count)
        
        # 计算聚合统计
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
        
        # 分桶统计
        buckets = self._create_buckets(
            activity_counts,
            [(0, 0), (1, 10), (11, 50), (51, 100), (101, 500), (501, 1000), (1001, float('inf'))],
            ['0', '1-10', '11-50', '51-100', '101-500', '501-1000', '1000+']
        )
        stats['activity_count_buckets'] = buckets
        
        print(f"  ✓ 平均每个人员 {stats['avg_activities_per_person']} 个活动")
        
        return stats
    
    def analyze_activity_types(self, account_ids: List[int]) -> Dict:
        """
        分析指定账户的活动类型分布
        """
        print(f"\n分析样本账户的活动类型分布...")
        
        ids_str = ','.join(str(id) for id in account_ids)
        
        query = f"""
        SELECT 
            activityType,
            COUNT(*) as count,
            COUNT(*) * 100.0 / (
                SELECT COUNT(*) 
                FROM activity 
                WHERE account_id IN ({ids_str})
                  AND activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
                  AND activityType IS NOT NULL
            ) as percentage
        FROM activity
        WHERE account_id IN ({ids_str})
          AND activity_date >= DATE_SUB(NOW(), INTERVAL {self.activity_time_range_days} DAY)
          AND activityType IS NOT NULL
        GROUP BY activityType
        ORDER BY count DESC
        LIMIT 20
        """
        
        result = self.execute_query(query)
        
        activity_types = [
            {
                'type_category': 'type_' + str(i),  # 不暴露实际类型名
                'count': row['count'],
                'percentage': round(row['percentage'], 2)
            }
            for i, row in enumerate(result)
        ]
        
        print(f"  ✓ 发现 {len(activity_types)} 种活动类型")
        
        return {
            'activity_type_distribution': activity_types,
            'total_types': len(activity_types)
        }
    
    def analyze_list_membership(self, account_ids: List[int]) -> Dict:
        """
        分析指定账户的列表成员关系
        """
        print(f"\n分析样本账户的列表成员关系...")
        
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
        
        # 构建统计
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
        
        print(f"  ✓ 平均每个账户在 {stats['avg_lists_per_account']} 个列表中")
        print(f"  ✓ {stats['accounts_not_in_lists']} 个账户不在任何列表中")
        
        return stats
    
    def _calculate_std(self, values: List[float]) -> float:
        """计算标准差"""
        if not values or len(values) < 2:
            return 0.0
        
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance ** 0.5
    
    def _create_buckets(self, values: List[int], ranges: List[tuple], labels: List[str]) -> List[Dict]:
        """
        创建分桶统计
        :param values: 数值列表
        :param ranges: 范围列表 [(min, max), ...]
        :param labels: 标签列表
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
        """执行完整的样本分析"""
        try:
            self.connect()
            
            print("\n" + "="*80)
            print("样本账户关系分析工具")
            print("="*80)
            print(f"\n配置:")
            print(f"  - 样本大小: {self.sample_size} 个账户")
            print(f"  - 活动时间范围: 最近 {self.activity_time_range_days} 天")
            print(f"  - 采样方法: {'指定ID' if self.account_ids else '随机采样'}")
            
            # 步骤1: 获取样本账户ID
            account_ids = self.get_sample_account_ids()
            self.results['metadata']['actual_sample_size'] = len(account_ids)
            self.results['metadata']['account_ids_sample'] = account_ids
            
            # 步骤2: 分析account-person关系
            person_stats = self.analyze_account_person_counts(account_ids)
            self.results['aggregated_stats']['account_person'] = person_stats
            
            # 步骤3: 分析account-activity关系
            activity_stats = self.analyze_account_activity_counts(account_ids)
            self.results['aggregated_stats']['account_activity'] = activity_stats
            
            # 步骤4: 分析person-activity关系
            person_activity_stats = self.analyze_person_activity_counts(account_ids)
            self.results['aggregated_stats']['person_activity'] = person_activity_stats
            
            # 步骤5: 分析列表成员关系
            list_stats = self.analyze_list_membership(account_ids)
            self.results['aggregated_stats']['account_list'] = list_stats
            
            # 步骤6: 分析活动类型分布
            type_stats = self.analyze_activity_types(account_ids)
            self.results['aggregated_stats']['activity_types'] = type_stats
            
            # 保存结果
            self.save_results(output_file)
            
            # 打印总结
            self.print_summary()
            
        except Exception as e:
            print(f"\n✗ 分析过程失败: {e}")
            raise
        finally:
            self.close()
    
    def save_results(self, output_file: str):
        """保存分析结果"""
        output_path = os.path.join(os.path.dirname(__file__), output_file)
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False, cls=DecimalEncoder)
        print(f"\n✓ 分析结果已保存到: {output_path}")
    
    def print_summary(self):
        """打印分析摘要"""
        print("\n" + "="*80)
        print("分析结果摘要")
        print("="*80)
        
        stats = self.results['aggregated_stats']
        
        if 'account_person' in stats:
            print(f"\nAccount-Person 关系:")
            print(f"  - 样本账户数: {stats['account_person']['sample_size']}")
            print(f"  - 平均人员数: {stats['account_person']['avg_persons_per_account']}")
            print(f"  - 标准差: {stats['account_person']['std_persons_per_account']}")
            print(f"  - 范围: {stats['account_person']['min_persons_per_account']} - {stats['account_person']['max_persons_per_account']}")
        
        if 'account_activity' in stats:
            print(f"\nAccount-Activity 关系 (最近{self.activity_time_range_days}天):")
            print(f"  - 样本账户数: {stats['account_activity']['sample_size']}")
            print(f"  - 平均活动数: {stats['account_activity']['avg_activities_per_account']}")
            print(f"  - 标准差: {stats['account_activity']['std_activities_per_account']}")
            print(f"  - 范围: {stats['account_activity']['min_activities_per_account']} - {stats['account_activity']['max_activities_per_account']}")
        
        if 'person_activity' in stats:
            print(f"\nPerson-Activity 关系 (最近{self.activity_time_range_days}天):")
            print(f"  - 样本人员数: {stats['person_activity']['sample_persons']}")
            print(f"  - 平均活动数: {stats['person_activity']['avg_activities_per_person']}")
            print(f"  - 标准差: {stats['person_activity']['std_activities_per_person']}")
        
        if 'account_list' in stats:
            print(f"\nAccount-List 成员关系:")
            print(f"  - 样本账户数: {stats['account_list']['sample_size']}")
            print(f"  - 平均列表数: {stats['account_list']['avg_lists_per_account']}")
            print(f"  - 未加入列表: {stats['account_list']['accounts_not_in_lists']} 个账户")
        
        print("\n" + "="*80)


def main():
    """主函数"""
    # 从环境变量读取账户ID（如果有的话）
    account_ids_str = os.getenv('ACCOUNT_IDS', '')
    account_ids = []
    if account_ids_str:
        # 支持逗号分隔的账户ID列表
        account_ids = [int(aid.strip()) for aid in account_ids_str.split(',') if aid.strip()]
        print(f"从环境变量读取到 {len(account_ids)} 个账户ID")
    
    # 从环境变量读取配置
    config = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'port': int(os.getenv('DB_PORT', '3306')),
        'user': os.getenv('DB_USER', 'root'),
        'password': os.getenv('DB_PASSWORD', ''),
        'database': os.getenv('DB_NAME', 'tenant'),
        # 采样配置
        'sample_size': int(os.getenv('SAMPLE_SIZE', '1000')),  # 默认1000个样本
        'activity_time_range_days': int(os.getenv('ACTIVITY_TIME_RANGE_DAYS', '90')),
        # 可选：指定具体的account_id列表
        'account_ids': account_ids  # 留空则随机采样
    }
    
    print("="*80)
    print("样本账户关系分析工具")
    print("="*80)
    print("\n特点:")
    print("  - 只分析指定的样本账户，避免大表JOIN")
    print("  - 使用 IN 子句精确定位，性能高")
    print("  - 对生产环境影响极小")
    print("  - 可以随机采样或指定账户ID")
    
    # 创建分析器并运行
    analyzer = SampleAccountAnalyzer(config)
    analyzer.run('sample_account_analysis.json')


if __name__ == '__main__':
    main()

