using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Drawing;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Forms;
// including the M2Mqtt Library
using uPLibrary.Networking.M2Mqtt;
using uPLibrary.Networking.M2Mqtt.Messages;
// including the MySQL Library
using MySql.Data.MySqlClient;

namespace MQTTMySQL
{
    public partial class Form1 : Form
    {

        delegate void UpdateDataGridViewCallback(string topic, string msg);//用來更新新增至MySQL的Callback

        //連線參數   (請依實際Mysql設定自行修改)
        //資料來源:本機localhost
        //port:3306
        //選擇某個資料庫:log
        //使用者名稱:loguser，密碼：123
        //建立MySQL連結參數
        MySqlConnection connection = new MySqlConnection("datasource=140.131.114.147;port=6969;Initial Catalog='temperature_log';username=root;password=Ihaveadream99!");

        delegate void SetTextCallback(string text);//用來更新UIText 的Callback

        MqttClient client;//MqttClient
        string clientId;//連線時所用的ClientID

        public Form1()
        {
            InitializeComponent();
        }
        //當視窗載入時觸發
        private void Form1_Load(object sender, EventArgs e)
        {
            client = new MqttClient("140.131.114.147");//MQTTServer在本機
            client.MqttMsgPublishReceived += client_MqttMsgPublishReceived;//當接收到訊息時處理函式
            clientId = Guid.NewGuid().ToString();//取得唯一碼
            client.Connect(clientId);//建立連線

            //===NEW=== 
            connection.Open(); //與MySQL建立連線並使用temperature_log資料庫

            SelectMySQL(connection, "sensor", dataGridView_sensor);// 查詢(temperature_log資料庫內的)sensor資料表

            //訂閱訊息主題與異常紀錄主題(在Load時就訂閱所以不必再用輸入文字框來訂閱(Sensor)主題)
            client.Subscribe(new string[] { "Sensor" }, new byte[] { 0 });   // we need arrays as parameters because we can subscribe to different topics with one call
            SetText("");//先將RecText.TextBox清空
            //===NEW===
        }
        //當視窗關閉時
        private void Form1_FormClosing(object sender, FormClosingEventArgs e)
        {
            client.Disconnect();//中斷連線

            //===NEW===
            connection.Close(); //與MySQL 中斷連線
            //===NEW===
        }

        //按下訂閱按鈕時觸發
        private void btnSubscribe_Click(object sender, EventArgs e)
        {   //若有輸入訂閱主題
            if (txtTopicSubscribe.Text != "")
            {
                //自訂完整主題名稱
                string Topic = txtTopicSubscribe.Text;

                //設定主題及傳送品質 0 ( 0, 1, 2 )
                client.Subscribe(new string[] { Topic }, new byte[] { 0 });   // we need arrays as parameters because we can subscribe to different topics with one call
                                                                              //清空接收文字框
                SetText("");
            }
            else
            {
                MessageBox.Show("必需輸入訂閱主題!");
            }
        }
        //按下發佈按鈕時觸發
        private void btnPublish_Click(object sender, EventArgs e)
        {
            //若有輸入發佈主題
            if (txtTopicPublish.Text != "")
            {
                //設定完整的發佈路徑
                string Topic = txtTopicPublish.Text;

                //發佈主題、內容及設定傳送品質 QoS 0 ( 0, 1, 2 )
                client.Publish(Topic, Encoding.UTF8.GetBytes(txtPublish.Text), MqttMsgBase.QOS_LEVEL_AT_MOST_ONCE, true);
            }
            else
            {
                MessageBox.Show("必需輸入發佈主題!");
            }
        }
        // this code runs when a message was received
        void client_MqttMsgPublishReceived(object sender, MqttMsgPublishEventArgs e)
        {
            //收到的訊息內容以UTF8編碼
            string ReceivedMessage = Encoding.UTF8.GetString(e.Message);

            // we need this construction because the receiving code in the library and the UI with textbox run on different threads
            //將訊息寫進接收訊息框內，但因為MQTT接收的執行緒與UI執行緒不同，我們需要呼叫自訂的SetText函式做些處理
            SetText(ReceivedMessage);

            //===NEW===
            string ReceivedTopic = e.Topic.ToLower();//收到的主題轉成小寫
                                                     // we need this construction because the receiving code in the library and the UI with DataGridView run on different threads
                                                     //將主題與訊息寫進datagridview框內，但因為MQTT接收的執行緒與UI執行緒不同，我們需要呼叫自訂的UpdateDataGridView函式做些處理
            UpdateDataGridView(ReceivedTopic, ReceivedMessage);
            //===NEW===
        }
        //當不同執行緒在UI執行緒上需要更新數值時的處理
        private void SetText(string text)
        {
            // we need this construction because the receiving code in the library and the UI with textbox run on different threads
            if (this.RecText.InvokeRequired)
            {   //如果需要Invoke
                //設定CallBack,Invoke
                SetTextCallback d = new SetTextCallback(SetText);
                this.Invoke(d, new object[] { text });
            }
            else
            {
                //若不需要Invoke直接設定其值
                this.RecText.Text = text;
            }
        }

        //查詢某一個資料表的方法
        private void SelectMySQL(MySqlConnection connection, string tablename, DataGridView datagrid)
        {
            //從 MYSQL 中查詢資料並於 dataGridView 中顯示 

            //查詢資料表的select語法
            string sql = "SELECT * FROM " + tablename + " ORDER BY _AI DESC LIMIT 5";
            //在記憶體建立新的table空白表格
            DataTable table = new DataTable();
            //MySqlDataAdapter類別用connection去查詢MySQL的資料
            MySqlDataAdapter adapter = new MySqlDataAdapter(sql, connection);
            //查詢後的adapter填入table
            adapter.Fill(table);
            //table顯示在dataGridView的DataSource
            datagrid.DataSource = table;
            //dataGridView欄位依照內容長短調整欄寬
            datagrid.AutoSizeColumnsMode = DataGridViewAutoSizeColumnsMode.Fill;
        }

        //新增Message資料至某個資料表的方法
        private void Insert(MySqlConnection connection, string tablename, string message)
        {
            String Record_time;//紀錄資料加入的時間
            Record_time = String.Format("{0:yyyy/MM/dd HH:mm:ss}", DateTime.Now);
            string sql = "INSERT INTO " + tablename + "(Record_time,Message) VALUES('" + Record_time + "','" + message + "')";
            MySqlCommand command = new MySqlCommand(sql, connection);
            command.ExecuteNonQuery();//執行SQL
        }

        //當兩個不同執行緒上需要更新數值(UI執行緒)時的處理
        private void UpdateDataGridView(string topic, string message)
        {
            // we need this construction because the receiving code in the library and the UI with DataGridView run on different threads
            if (this.InvokeRequired)
            {
                //如果需要Invoke
                //設定CallBack,Invoke
                UpdateDataGridViewCallback d = new UpdateDataGridViewCallback(UpdateDataGridView);
                this.Invoke(d, new object[] { topic, message });
            }
            else
            {
                //若不需要Invoke直接新增至MySQL
                //新增資料至主題對應的資料表
                Insert(connection, topic, message);

                //查詢主題對應的資料表
                SelectMySQL(connection, topic, (DataGridView)this.Controls.Find("dataGridView_" + topic, true)[0]);
            }
        }

    }
}
