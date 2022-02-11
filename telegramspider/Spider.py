import re
import pandas as pd
from lxml import html
from loguru import logger as log

class Spider(object):

    def __init__(self) -> None:
        pass

    def start():
        pass

    def restart():
        pass

    def get_node(self, node) -> any:
        pass

    def get_neighbors(self, node: str) -> any:
        pass

    def load_config(self) -> None:
        pass

    def spider(self) -> None:
        #
        pass


    def spider_old(self, account: str or list(str), con, max_iteration = 1000, branches = 150, restart = False):
    
        def visit(node: str) -> None:
            visit_url = f'https://t.me/s/{node}'
            log.info(f'visiting {visit_url}')
            message_data = None
            
            resp = requests.get(visit_url)
            if resp.status_code == 302:
                log.warn(f'{visit_url} throw an error')
                # TODO: remove this node
                return
            
            html_source = html.fromstring(resp.content)
            try:
                message_data = parse_tg_channel(html_source)
                user_data = parse_tg_user(html_source)
                
                log.info(f'got {message_data.shape[0]} messages and {user_data.shape[0]} users')
            except:
                log.warning(f'parsing failed for {node}')
                user_data = pd.DataFrame([(node)], columns = ['handle'])
                user_data.to_sql('node_list', con, if_exists='append', index_label='handle', index = False)
                return
            
            user_data.to_sql('node_list', con, if_exists='append', index_label='handle', index = False)
            if message_data is not None:
                message_data.to_sql('messages', con, if_exists='append', index_label = 'post_id', index = False)
                
                # grab and copy all links
                edges = message_data[['link']]
                # unnest individual links
                edges = pd.DataFrame(
                    edges.link.str.\
                    split(',', expand=False).\
                    explode(0)
                ).reset_index(drop=True)
                edges["to"]   = edges.link.str.extract('https://t.me/(\w+)')
                edges         = edges.dropna().drop('link', axis = 1)
                edges["from"] = node
                
                edges.to_sql("edge_list", con, if_exists='append', index = False)
            
        # setup db, intialize everything
        log.info(f'starting collection for {account}')
        
        iteration = 0
        
        if restart == False:
            if hasattr(account, '__len__') and len(account) > 1:
                [visit(_) for _ in account]
            else:
                visit(account)
        
        while iteration < max_iteration:
            # get new candidate nodes ordered by in degree
            new_edges = pd.read_sql(f'SELECT "to", COUNT(*) as "count" FROM edge_list el LEFT JOIN node_list nl ON "to" = "handle" WHERE "handle" IS NULL GROUP BY "to" ORDER BY "count" DESC LIMIT {branches};', con)
            #
            if new_edges.shape[0] == 0:
                log.info(f'no new nodes to visit: stopping.')
                return
            [visit(_) for _ in new_edges.to]
            
            log.info(f'starting iteration {iteration}')
            iteration += 1
        