import json
import requests
import datetime
import pandas as pd
from time import sleep
import os
import re
import hashlib
from collections import Counter

def useridToUsername(id):
    if str(id).isnumeric():
        r1 = requests.get('https://instagram.com/instagram/', headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0', }).text
        rhx_gis = json.loads(re.compile('window._sharedData = ({.*?});', re.DOTALL).search(r1).group(1))['nonce']

        ppc = re.search(r'ProfilePageContainer.js/(.*?).js', r1).group(1)
        r2 = requests.get('https://www.instagram.com/static/bundles/es6/ProfilePageContainer.js/' + ppc + '.js').text
        query_hash = re.findall(r'{value:!0}\);const o=\"(.*?)\"', r2)[0]

        query_variable = '{"user_id":"' + str(id) + '","include_reel":true}'
        t = rhx_gis + ':' + query_variable
        x_instagram_gis = hashlib.md5(t.encode("utf-8")).hexdigest()

        header = {'X-Instagram-GIS': x_instagram_gis,
                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:64.0) Gecko/20100101 Firefox/64.0',
                  'X-Requested-With': 'XMLHttpRequest'}
        r3 = requests.get(
            'https://www.instagram.com/graphql/query/?query_hash=' + query_hash + '&variables=' + query_variable,
            headers=header).text

        username = json.loads(r3)['data']['user']['reel']['user']['username']
        return username


def instagram_data_collection(searched_tag, num_data):
    """
    colllect the needed data for the interested hashtag
    ===================================================
    INPUT:
    searched_tag : str
    ---------------------------------------------------
    OUTPUT:
    tags_data : Pandas DataFrame with 
               -username : the username of the post owner hashtag  ,str
               -searched_tag : the name of the requested hashtag  ,str
               -related key words: related hashtags written in the post, list 
               -number of comments:
               -number of likes:
               -date of post: datetime
               -is_vedio : True or False
               -is_top : to be one of the top posts , True or False
               
    {searched_tag}_data.csv file : convert pandas DataFrame to .csv file

    """
    # count_posts
    count_posts = 0
    # get the json link for the needed hashtag
    url = 'https://www.instagram.com/explore/tags/'+ searched_tag + '/?__a=1'
    response = requests.get(url)
    response_text = response.text
    data = json.loads(response_text)

    #loop through the old posts
    all_data = []
    all_data.append(data)
    
    # total feteched posts
    count_posts = len(data['graphql']['hashtag']['edge_hashtag_to_media']['edges'])

    has_next_page = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['has_next_page']
    # get all data
    while has_next_page:
        if count_posts >= num_data:
            break
        else:    
            # end cursor to be put after (max_id = ) to go to next page
            next_page = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['end_cursor']
            new_url = url + '&max_id=' + next_page
            # parse the interested url
            sleep(2)
            response = requests.get(new_url)
            response_text = response.text
            # load json file
            data = json.loads(response_text)
            count_posts = count_posts + len(data['graphql']['hashtag']['edge_hashtag_to_media']['edges'])
            
            all_data.append(data)
            has_next_page = data['graphql']['hashtag']['edge_hashtag_to_media']['page_info']['has_next_page']

    print('{} posts has been fetched....'.format(count_posts))
    print('Extracting data....')
    #get post_details
    users_id,usernames,captions,links,num_tags,num_comments,num_likes,post_dates,is_vedios,is_tops,x_tags = [],[],[],[],[],[],[],[],[],[],[]
    #loop through all collected data
    count_posts = 0
    for data in all_data:
        
        if count_posts >= num_data:
            break
        
        # arrange the data needed either from top_posts or normal posts 
        top_posts = data['graphql']['hashtag']['edge_hashtag_to_top_posts']['edges']
        posts = data['graphql']['hashtag']['edge_hashtag_to_media']['edges']
        all_posts = [top_posts,posts]
        for i in all_posts:

            if count_posts >= num_data:
                break


            if i is top_posts:
                # is_top
                is_top = True
            else:
                is_top = False
                
            # looping through both lists at once  
            for _ in i:
                
                if count_posts >= num_data:
                    break

                # the post content
                post = _['node']
                
                # other hashtags/key words appear on the post
                try:
                    text = post['edge_media_to_caption']['edges'][0]['node']['text']
                    x_tag = [i for i in text.split() if i.startswith('#')] 
                except Exception:
                    continue
                # user_id
                user_id = post['owner']['id']

                # get username from user_id
                username = useridToUsername(user_id)
               
                
                # link to the post
                post_link = 'https://www.instagram.com/p/'+post['shortcode'] + '/'
                # number of comments on the post
                num_comment = post['edge_media_to_comment']['count']
                # number of likes 
                num_like = post['edge_liked_by']['count']
                # the post date
                post_date = datetime.datetime.fromtimestamp(post['taken_at_timestamp']).strftime("%Y-%m-%d")
                # video
                is_vedio = post['is_video']
                # gather other-tags data
                index = 0
                x_tag_str=''
                while index < len(x_tag):
                    x_tag[index] = x_tag[index].strip('#')
                    index += 1
                    x_tag_str = ','.join(x_tag)
                x_tags.append(x_tag_str)
                num_tags.append(len(x_tag))
                
                
                users_id.append(user_id)
                usernames.append(username)
                captions.append(text)
                links.append(post_link)
                num_comments.append(num_comment)
                num_likes.append(num_like)
                post_dates.append(post_date)
                is_vedios.append(is_vedio)
                is_tops.append(is_top)

                # print
                print('{} #{} username:{}'.format(count_posts,searched_tag,username))
                count_posts = count_posts + 1
                

    tags_data = pd.DataFrame({'usernames':usernames,u'searched_tag':searched_tag,'all_tags':x_tags,'content':captions,'num_tags':num_tags,'num_comments':num_comments, 
                          'num_likes':num_likes,'date':post_dates,'is_vedios':is_vedios,
                          'is_top_post':is_tops,'link':links}, 
                          columns=[u'usernames',u'searched_tag',u'all_tags',u'content','num_tags','num_comments','is_top_post','link',])
    
    #tags_data.searched_tag = tags_data.searched_tag.str.encode('utf-8')


    # replace the empty string eith none values
    tags_data.all_tags = tags_data.all_tags.apply(lambda x : None if x == '' else x)

    #save to csv file to the working directory
    path_to_data = f'{searched_tag}_data.csv'
    # if needed to be saved to a certain directory uncomment the following
    #path = r""
    #path_to_data = os.path.join(path,path_to_data) 

    # save data
    tags_data.to_csv(path_to_data, sep=',', header=True, encoding='utf-8-sig', index= False)
    
    print('Data collection has been completed.')
    return tags_data

if __name__ == "__main__":
    _searched_tag = str(input('Enter the hashtag:'))
    _num_data = int(input('Enter the number of posts:'))
    print('Collecting data....')
    data = instagram_data_collection(_searched_tag, _num_data)
    print('Done...')
