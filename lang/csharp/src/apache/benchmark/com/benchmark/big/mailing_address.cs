// ------------------------------------------------------------------------------
// <auto-generated>
//    Generated by avrogen, version 1.11.0.0
//    Changes to this file may cause incorrect behavior and will be lost if code
//    is regenerated
// </auto-generated>
// ------------------------------------------------------------------------------
namespace com.benchmark.big
{
	using System;
	using System.Collections.Generic;
	using System.Text;
	using Avro;
	using Avro.Specific;
	
	public partial class mailing_address : ISpecificRecord
	{
		public static Schema _SCHEMA = Avro.Schema.Parse(@"{""type"":""record"",""name"":""mailing_address"",""namespace"":""com.benchmark.big"",""fields"":[{""name"":""street"",""default"":""NONE"",""type"":""string""},{""name"":""city"",""default"":""NONE"",""type"":""string""},{""name"":""state_prov"",""default"":""NONE"",""type"":""string""},{""name"":""country"",""default"":""NONE"",""type"":""string""},{""name"":""zip"",""default"":""NONE"",""type"":""string""}]}");
		private string _street;
		private string _city;
		private string _state_prov;
		private string _country;
		private string _zip;
		public virtual Schema Schema
		{
			get
			{
				return mailing_address._SCHEMA;
			}
		}
		public string street
		{
			get
			{
				return this._street;
			}
			set
			{
				this._street = value;
			}
		}
		public string city
		{
			get
			{
				return this._city;
			}
			set
			{
				this._city = value;
			}
		}
		public string state_prov
		{
			get
			{
				return this._state_prov;
			}
			set
			{
				this._state_prov = value;
			}
		}
		public string country
		{
			get
			{
				return this._country;
			}
			set
			{
				this._country = value;
			}
		}
		public string zip
		{
			get
			{
				return this._zip;
			}
			set
			{
				this._zip = value;
			}
		}
		public virtual object Get(int fieldPos)
		{
			switch (fieldPos)
			{
			case 0: return this.street;
			case 1: return this.city;
			case 2: return this.state_prov;
			case 3: return this.country;
			case 4: return this.zip;
			default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Get()");
			};
		}
		public virtual void Put(int fieldPos, object fieldValue)
		{
			switch (fieldPos)
			{
			case 0: this.street = (System.String)fieldValue; break;
			case 1: this.city = (System.String)fieldValue; break;
			case 2: this.state_prov = (System.String)fieldValue; break;
			case 3: this.country = (System.String)fieldValue; break;
			case 4: this.zip = (System.String)fieldValue; break;
			default: throw new AvroRuntimeException("Bad index " + fieldPos + " in Put()");
			};
		}
	}
}
