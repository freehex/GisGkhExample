using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using GisGkhApi.HouseManagementService;
using DBSetExtension;

namespace GisGkhApi.Data
{
    public class DataAccount: DataBase
    {
        #region Properties
        /// <summary>
        /// Плательщики
        /// </summary>
        public List<DataPayer> Payers { get; set; }
        /// <summary>
        /// Помещения
        /// </summary>
        public List<DataPremise> Premises { get; set; }
        /// <summary>
        /// Комнаты
        /// </summary>
        public List<DataLivingRoom> Rooms { get; set; }
        /// <summary>
        /// Номер ЛС
        /// </summary>
        public string Number { get; set; }
        /// <summary>
        /// Уникальный идентификатор
        /// </summary>
        public long Id { get; set; }
        /// <summary>
        /// Дата начала действия ЛС
        /// </summary>
        public DateTime? CreationDate { get; set; }
        /// <summary>
        /// Суммарная площадь
        /// </summary>
        public decimal? TotalSquare { get; set; }
        /// <summary>
        /// Количество проживающих
        /// </summary>
        public int? LivingPersonsNumber { get; set; }

        /// <summary>
        /// Отапливаемая площадь
        /// </summary>
        public decimal? HeatedSquare { get; set; }

        /// <summary>
        /// Жилая площадь
        /// </summary>
        public decimal? LivingSquare { get; set; }
        /// <summary>
        /// Тип лицевого счета
        /// </summary>
        public Enums.AccountType Type { get; set; }

        public string GUID { get; internal set; }
        /// <summary>
        /// Дом, полученный из АПИ. Сделал свойством, чтобы в каждом приборе учета не грузить его с сайта ГИС, будем делать это выше в DataMeteringDeviceBase
        /// </summary>
        public DataHouse House { get; set; }
        #endregion

        #region Constructor
        public DataAccount(ProfileDataHelper dataHelper, object rowData) : base(dataHelper, rowData)
        { }
        public DataAccount(ProfileDataHelper dataHelper, string homeFIAS, object apiData = null, bool immediatelyExport = true) : base(dataHelper, homeFIAS, apiData, immediatelyExport)
        { }
        #endregion

        #region Methods
        public override void FillFromDB()
        {
            if (RowData == null)
                throw new Exception("RowData не может быть null");

            CreationDate = GetDBValue<DateTime?>("ACCOUNTDATA_CREATIONDATE");
            Id = GetDBValue<long>("ACCOUNTDATA_ID");
            Number = GetDBValue<string>("ACCOUNTDATA_NUMBER");
            TotalSquare = GetDBValue<Decimal>("ACCOUNTDATA_TOTALSQUARE");
            LivingSquare = GetDBValue<decimal>("ACCOUNTDATA_LIVESQUARE");
            HeatedSquare = GetDBValue<decimal?>("ACCOUNTDATA_HEATQUARE");
            LivingPersonsNumber = GetDBValue<int?>("ACCOUNTDATA_LIVECNT");
            int? type = GetDBValue<int?>("ACCOUNTDATA_TYPE");
            this.Type = (Enums.AccountType)type;

            FillPayers();
            FillPremises();
            FillRooms();
        }
        private void FillPayers()
        {
            //Получаем полный список плательщиков по лицевому счету
            StringBuilder getAllPayersCommand = new StringBuilder();
            getAllPayersCommand.Append("SELECT DISTINCT PAYER.* FROM ");
            getAllPayersCommand.Append("ACCOUNTDATA INNER JOIN ACCOUNTTOPAYER ON ACCOUNTTOPAYER.ACCOUNTDATA_ID = ACCOUNTDATA.ACCOUNTDATA_ID ");
            getAllPayersCommand.Append("INNER JOIN PAYER ON ACCOUNTTOPAYER.PAYER_ID = PAYER.PAYER_ID ");
            getAllPayersCommand.Append("WHERE ACCOUNTDATA.ACCOUNTDATA_ID = @account_id");
            Client.set.AddSQLParameter("@account_id", Id);

            var payerRows = Client.set.GetView(getAllPayersCommand.ToString());

            Payers = new List<DataPayer>();

            foreach (var payerRow in payerRows)
            {
                Payers.Add(new DataPayer(DataHelper, payerRow));
            }
        }
        private void FillPremises()
        {
            //получаем полный список помещений по лицевому счету
            var getAllPremisesCommand = new StringBuilder();
            getAllPremisesCommand.Append("SELECT PREMISES.*, ACCOUNTTOPREMISES.ACCOUNTTOPREMISES_SHAREPERCENT FROM ");
            getAllPremisesCommand.Append("ACCOUNTDATA INNER JOIN ACCOUNTTOPREMISES ON ACCOUNTTOPREMISES.ACCOUNTDATA_ID = ACCOUNTDATA.ACCOUNTDATA_ID ");
            getAllPremisesCommand.Append("INNER JOIN PREMISES ON ACCOUNTTOPREMISES.PREMISES_ID = PREMISES.PREMISES_ID ");
            getAllPremisesCommand.Append("WHERE ACCOUNTDATA.ACCOUNTDATA_ID = @account_id");
            Client.set.AddSQLParameter("@account_id", Id);

            var premisesRows = Client.set.GetView(getAllPremisesCommand.ToString());

            Premises = new List<DataPremise>();

            foreach (var premiseRow in premisesRows)
            {
                
                //TODO: 1 - жилое, 2 - нежилое
                var premiseType = GetDBValue<int>("PREMISESTYPE_ID", premiseRow);

                if (premiseType == 1)
                    Premises.Add(new DataResidentalPremise(DataHelper, premiseRow)
                    {
                        SharedPercent = GetDBValue<decimal?>("ACCOUNTTOPREMISES_SHAREPERCENT", premiseRow)
                    });
                else if (premiseType == 2)
                    Premises.Add(new DataNonResidentalPremise(DataHelper, premiseRow)
                    {
                        SharedPercent = GetDBValue<decimal?>("ACCOUNTTOPREMISES_SHAREPERCENT", premiseRow)
                    });
                else
                    throw new Exception("Неопознанный тип помещения"); //TODO: возможно ограничиться только записью в лог
            }
        }
        private void FillRooms()
        {
            //получаем полный список комнат по лицевому счету
            var command = new StringBuilder();
            command.Append("SELECT PREMISESLIVINGROOM.* FROM ");
            command.Append("ACCOUNTDATA INNER JOIN ACCOUNTTOLIVEROOM ON ACCOUNTTOLIVEROOM.ACCOUNTDATA_ID = ACCOUNTDATA.ACCOUNTDATA_ID ");
            command.Append("INNER JOIN PREMISESLIVINGROOM ON ACCOUNTTOLIVEROOM.PREMISESLIVINGROOM_ID = PREMISESLIVINGROOM.PREMISESLIVINGROOM_ID ");
            command.Append("WHERE ACCOUNTDATA.ACCOUNTDATA_ID = @account_id");
            Client.set.AddSQLParameter("@account_id", Id);

            var roomRows = Client.set.GetView(command.ToString());

            Rooms = roomRows.Select(roomRow => new DataLivingRoom(DataHelper, roomRow)).ToList();
        }

        public void SaveToDB()
        {
            var rowACCOUNTDATA = Set.GetRowOrNew("ACCOUNTDATA", "ACCOUNTDATA_NUMBER", Number);
            rowACCOUNTDATA["ACCOUNTDATA_CREATIONDATE"] = CreationDate;
            rowACCOUNTDATA["ACCOUNTDATA_TOTALSQUARE"] = TotalSquare;
            rowACCOUNTDATA["ACCOUNTDATA_LIVESQUARE"] = LivingSquare;
            rowACCOUNTDATA["ACCOUNTDATA_HEATQUARE"] = HeatedSquare;
            rowACCOUNTDATA["ACCOUNTDATA_LIVECNT"] = LivingPersonsNumber;
            rowACCOUNTDATA["ACCOUNTDATA_TYPE"] = (int)Type;
            Set.Save();
            if (Payers != null)
            {
                foreach (var payer in Payers)
                {
                    var rowPAYER = payer.SaveToDB();

                    var rowACCOUNTTOPAYER = Set.NewRow("ACCOUNTTOPAYER");
                    rowACCOUNTTOPAYER["ACCOUNTDATA_ID"] = rowACCOUNTDATA["ACCOUNTDATA_ID"];
                    rowACCOUNTTOPAYER["PAYER_ID"] = rowPAYER["PAYER_ID"];
                }
            }
            if (Premises != null)
            {
                foreach (var premise in Premises)
                {
                    var rowACCOUNTTOPREMISES = Set.NewRow("ACCOUNTTOPREMISES");
                    rowACCOUNTTOPREMISES["ACCOUNTTOPREMISES_SHAREPERCENT"] = premise.SharedPercent;

                    var rowPREMISES = Set.GetRowOrNew("PREMISES", "MKD_ID", MKD_ID, "PREMISES_PREMISESNUM", premise.Number);
                    rowACCOUNTTOPREMISES["PREMISES_ID"] = rowPREMISES["PREMISES_ID"];
                    rowACCOUNTTOPREMISES["ACCOUNTDATA_ID"] = rowACCOUNTDATA["ACCOUNTDATA_ID"];
                }
            }
            if (Rooms != null)
            {
                foreach (var room in Rooms)
                {
                    var rowACCOUNTTOLIVEROOM = Set.NewRow("ACCOUNTTOLIVEROOM");

                    var rowPREMISES = Set.GetRowOrNew("PREMISES", "MKD_ID", MKD_ID, "PREMISES_PREMISESNUM", room.PremiseNumber);
                    var rowPREMISESLIVINGROOM = Set.GetRowOrNew("PREMISESLIVINGROOM", "PREMISES_ID", rowPREMISES["PREMISES_ID"]);

                    rowACCOUNTTOLIVEROOM["PREMISESLIVINGROOM_ID"] = rowPREMISESLIVINGROOM["PREMISESLIVINGROOM_ID"];
                    rowACCOUNTTOLIVEROOM["ACCOUNTDATA_ID"] = rowACCOUNTDATA["ACCOUNTDATA_ID"];
                }
            }
            Set.Save();
            //Client.OnLog($"Запись о ЛС № {Number} добавлена в БД");
        }

        public override void FillFromApi(object apiData, string homeFIAS = null)
        {
            if (apiData == null || (apiData as exportAccountResultType) == null)
                throw new Exception("apiData не может быть null и должен иметь тип exportAccountResultType");

            var accountData = apiData as exportAccountResultType;

            Number = accountData.AccountNumber;
            GUID = accountData.AccountGUID;
            CreationDate = accountData.CreationDate;
            LivingPersonsNumber = accountData.LivingPersonsNumberSpecified ? accountData.LivingPersonsNumber : (int?)null;
            TotalSquare = accountData.TotalSquareSpecified ? accountData.TotalSquare : (decimal?)null;
            LivingSquare = accountData.ResidentialSquareSpecified ? accountData.ResidentialSquare : (decimal?)null;
            HeatedSquare = accountData.HeatedAreaSpecified ? accountData.HeatedArea : (decimal?)null;
            Type = GetAccountExportType(accountData.ItemElementName);

            var premiseGUIDs = accountData.Accommodation?.Where(x => x.ItemElementName == ItemChoiceType7.PremisesGUID).Select(x => x.Item).ToList();
            var roomGUIDs = accountData.Accommodation?.Where(x => x.ItemElementName == ItemChoiceType7.LivingRoomGUID).Select(x => x.Item).ToList();

            var premises = new List<DataPremise>();
            var rooms = new List<DataLivingRoom>();
            var payers = new List<DataPayer>();

            foreach (var premiseGUID in premiseGUIDs)
            {
                var premise = House.Premises?.Where(x => String.CompareOrdinal(x.GUID, premiseGUID) == 0).FirstOrDefault();

                if (premise != null)
                {
                    premises.Add(premise);
                }
                else
                    Client.OnLog($"На сайте ГИС не найдено помещение, связанное с ЛС № {Number}, имеющее GUID = {premiseGUID}");
            }

            foreach (var roomGUID in roomGUIDs)
            {
                var room = House.Premises.OfType<DataResidentalPremise>()?.SelectMany(x => x.Rooms)?.Where(x => String.CompareOrdinal(x.GUID, roomGUID) == 0).FirstOrDefault();

                if (room != null)
                {
                    rooms.Add(room);
                }
                else
                    Client.OnLog($"На сайте ГИС не найдено комнаты, связанной с ЛС № {Number}, имеющей GUID = {roomGUID}");
            }

            if (accountData.PayerInfo != null)
            { //пока работаем только с физ.лицами, т.к. в импорте ЛС пока юр.лица не используются
                payers.Add(new DataPayer(DataHelper, HomeFIAS, accountData.PayerInfo));
            }

            if (premises?.Count > 0)
                Premises = premises;

            if (rooms?.Count > 0)
                Rooms = rooms;

            if (payers?.Count > 0)
                Payers = payers;
        }
        public exportHouseResult exportHouse;
        public List<importAccountRequestAccount> GetCreateData()
        {
            var result = new List<importAccountRequestAccount>();

            foreach (var payer in Payers)
            {
                var accountRequestAccount = new importAccountRequestAccount();
                accountRequestAccount.AccountNumber = Number;
                accountRequestAccount.TransportGUID = ServiceHelper.GenerateGUID();
                accountRequestAccount.CreationDate = CreationDate.GetValueOrDefault();
                accountRequestAccount.CreationDateSpecified = CreationDate.HasValue;

                accountRequestAccount.Item = true;
                accountRequestAccount.ItemElementName = GetAccountImportType(this.Type);
                accountRequestAccount.PayerInfo = payer.GetCreateData();

                accountRequestAccount.TotalSquare = TotalSquare.GetValueOrDefault();
                accountRequestAccount.TotalSquareSpecified = TotalSquare.HasValue;

                accountRequestAccount.HeatedArea = HeatedSquare.GetValueOrDefault();
                accountRequestAccount.HeatedAreaSpecified = HeatedSquare.HasValue;

                accountRequestAccount.ResidentialSquare = LivingSquare.GetValueOrDefault();
                accountRequestAccount.ResidentialSquareSpecified = LivingSquare.HasValue;

                accountRequestAccount.LivingPersonsNumber = (sbyte)LivingPersonsNumber.GetValueOrDefault();
                accountRequestAccount.LivingPersonsNumberSpecified = LivingPersonsNumber.HasValue;

                var accommodationList = new List<AccountTypeAccommodation>();

                foreach (var p in Premises)
                {
                    p.exportHouse = exportHouse;
                }
                foreach (var p in Rooms)
                {
                    p.exportHouse = exportHouse;
                }
                //помещения:
                accommodationList.AddRange(Premises.SelectMany(x => x.GetAccountData()).ToList());
                //комнаты:
                accommodationList.AddRange(Rooms.SelectMany(x => x.GetAccountData()).ToList());

                accountRequestAccount.Accommodation = accommodationList.ToArray();

                result.Add(accountRequestAccount);
            }
            
            return result;
        }

        private ItemChoiceType6 GetAccountImportType(Enums.AccountType type)
        {
            return compareDic.Single(p=>p.Value == type).Key;
        }
        private Enums.AccountType GetAccountExportType(ItemChoiceType6 type)
        {
            return compareDic.Single(p =>p.Key == type).Value;
        }
        private static readonly Dictionary<ItemChoiceType6, Enums.AccountType> compareDic = new Dictionary<ItemChoiceType6, Enums.AccountType>
        {
            { ItemChoiceType6.isCRAccount, Enums.AccountType.isCRAccount },
            { ItemChoiceType6.isOGVorOMSAccount, Enums.AccountType.isOGVorOMSAccount },
            { ItemChoiceType6.isRCAccount, Enums.AccountType.isRCAccount },
            { ItemChoiceType6.isRSOAccount, Enums.AccountType.isRSOAccount },
            { ItemChoiceType6.isUOAccount, Enums.AccountType.isUOAccount }
        };

        public List<importAccountRequestAccount> GetUpdateData(string accountGUID)
        {
            var result = GetCreateData();
            foreach (var accountData in result)
            {
                accountData.AccountGUID = accountGUID; //а это нужно когда обновляем
            }
            return result;
        }
        #endregion
    }
}
