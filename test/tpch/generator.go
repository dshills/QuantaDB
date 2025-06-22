package tpch

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

// Generator generates TPC-H compliant test data
type Generator struct {
	scaleFactor float64
	seed        int64
	rng         *rand.Rand
}

// NewGenerator creates a new TPC-H data generator
func NewGenerator(scaleFactor float64) *Generator {
	seed := time.Now().UnixNano()
	return &Generator{
		scaleFactor: scaleFactor,
		seed:        seed,
		rng:         rand.New(rand.NewSource(seed)),
	}
}

// Region data (5 fixed regions)
var regions = []struct {
	key     int
	name    string
	comment string
}{
	{0, "AFRICA", "lar deposits. blithely final packages cajole. regular waters are final requests. regular accounts are according to"},
	{1, "AMERICA", "hs use ironic, even requests. s"},
	{2, "ASIA", "ges. thinly even pinto beans ca"},
	{3, "EUROPE", "ly final courts cajole furiously final excuse"},
	{4, "MIDDLE EAST", "uickly special accounts cajole carefully blithely close requests. carefully final asymptotes haggle furiousl"},
}

// Nation data (25 fixed nations)
var nations = []struct {
	key       int
	name      string
	regionKey int
}{
	{0, "ALGERIA", 0}, {1, "ARGENTINA", 1}, {2, "BRAZIL", 1}, {3, "CANADA", 1}, {4, "EGYPT", 4},
	{5, "ETHIOPIA", 0}, {6, "FRANCE", 3}, {7, "GERMANY", 3}, {8, "INDIA", 2}, {9, "INDONESIA", 2},
	{10, "IRAN", 4}, {11, "IRAQ", 4}, {12, "JAPAN", 2}, {13, "JORDAN", 4}, {14, "KENYA", 0},
	{15, "MOROCCO", 0}, {16, "MOZAMBIQUE", 0}, {17, "PERU", 1}, {18, "CHINA", 2}, {19, "ROMANIA", 3},
	{20, "SAUDI ARABIA", 4}, {21, "VIETNAM", 2}, {22, "RUSSIA", 3}, {23, "UNITED KINGDOM", 3}, {24, "UNITED STATES", 1},
}

// Market segments
var marketSegments = []string{"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"}

// Order priorities
var orderPriorities = []string{"1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"}

// GenerateRegions generates region data
func (g *Generator) GenerateRegions() []string {
	var inserts []string
	for _, r := range regions {
		insert := fmt.Sprintf("INSERT INTO region VALUES (%d, '%s', '%s');",
			r.key, r.name, r.comment)
		inserts = append(inserts, insert)
	}
	return inserts
}

// GenerateNations generates nation data
func (g *Generator) GenerateNations() []string {
	var inserts []string
	for _, n := range nations {
		comment := g.randomText(31, 114)
		insert := fmt.Sprintf("INSERT INTO nation VALUES (%d, '%s', %d, '%s');",
			n.key, n.name, n.regionKey, comment)
		inserts = append(inserts, insert)
	}
	return inserts
}

// GenerateSuppliers generates supplier data
func (g *Generator) GenerateSuppliers() []string {
	count := int(10000 * g.scaleFactor)
	var inserts []string

	for i := 1; i <= count; i++ {
		name := fmt.Sprintf("Supplier#%09d", i)
		address := g.randomText(10, 40)
		nationKey := g.rng.Intn(25)
		phone := g.randomPhone(nationKey)
		acctbal := g.randomDecimal(-999.99, 9999.99)
		comment := g.randomText(25, 100)

		insert := fmt.Sprintf("INSERT INTO supplier VALUES (%d, '%s', '%s', %d, '%s', %.2f, '%s');",
			i, name, address, nationKey, phone, acctbal, comment)
		inserts = append(inserts, insert)
	}
	return inserts
}

// GenerateCustomers generates customer data
func (g *Generator) GenerateCustomers() []string {
	count := int(150000 * g.scaleFactor)
	var inserts []string

	for i := 1; i <= count; i++ {
		name := fmt.Sprintf("Customer#%09d", i)
		address := g.randomText(10, 40)
		nationKey := g.rng.Intn(25)
		phone := g.randomPhone(nationKey)
		acctbal := g.randomDecimal(-999.99, 9999.99)
		mktsegment := marketSegments[g.rng.Intn(len(marketSegments))]
		comment := g.randomText(29, 116)

		insert := fmt.Sprintf("INSERT INTO customer VALUES (%d, '%s', '%s', %d, '%s', %.2f, '%s', '%s');",
			i, name, address, nationKey, phone, acctbal, mktsegment, comment)
		inserts = append(inserts, insert)
	}
	return inserts
}

// GenerateParts generates part data
func (g *Generator) GenerateParts() []string {
	count := int(200000 * g.scaleFactor)
	var inserts []string

	types := []string{"ECONOMY", "PROMO", "STANDARD", "SMALL", "MEDIUM"}
	containers := []string{"SM CASE", "SM BOX", "SM BAG", "SM JAR", "MD CASE", "MD BOX", "MD BAG", "MD JAR",
		"LG CASE", "LG BOX", "LG BAG", "LG JAR", "JUMBO CASE", "JUMBO BOX", "JUMBO BAG", "JUMBO JAR"}

	for i := 1; i <= count; i++ {
		name := g.randomPartName()
		mfgr := fmt.Sprintf("Manufacturer#%d", g.rng.Intn(5)+1)
		brand := fmt.Sprintf("Brand#%d%d", g.rng.Intn(5)+1, g.rng.Intn(5)+1)
		ptype := types[g.rng.Intn(len(types))] + " " + g.randomText(10, 20)
		size := g.rng.Intn(50) + 1
		container := containers[g.rng.Intn(len(containers))]
		retailPrice := 900.0 + float64(i%1000)/10.0
		comment := g.randomText(5, 22)

		insert := fmt.Sprintf("INSERT INTO part VALUES (%d, '%s', '%s', '%s', '%s', %d, '%s', %.2f, '%s');",
			i, name, mfgr, brand, ptype, size, container, retailPrice, comment)
		inserts = append(inserts, insert)
	}
	return inserts
}

// GenerateOrders generates order data with date handling
func (g *Generator) GenerateOrders() []string {
	count := int(1500000 * g.scaleFactor)
	var inserts []string

	// Date range: 1992-01-01 to 1998-12-31
	startDate := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
	endDate := time.Date(1998, 12, 31, 0, 0, 0, 0, time.UTC)
	dateRange := int(endDate.Sub(startDate).Hours() / 24)

	for i := 1; i <= count; i++ {
		custKey := g.rng.Intn(int(150000*g.scaleFactor)) + 1
		orderStatus := "O"
		if g.rng.Float64() < 0.5 {
			orderStatus = "F"
		}

		// Generate order date
		daysOffset := g.rng.Intn(dateRange)
		orderDate := startDate.AddDate(0, 0, daysOffset)

		totalPrice := g.randomDecimal(1000.0, 500000.0)
		orderPriority := orderPriorities[g.rng.Intn(len(orderPriorities))]
		clerk := fmt.Sprintf("Clerk#%09d", g.rng.Intn(1000)+1)
		shipPriority := 0
		comment := g.randomText(19, 78)

		insert := fmt.Sprintf("INSERT INTO orders VALUES (%d, %d, '%s', %.2f, '%s', '%s', '%s', %d, '%s');",
			i, custKey, orderStatus, totalPrice, orderDate.Format("2006-01-02"),
			orderPriority, clerk, shipPriority, comment)
		inserts = append(inserts, insert)
	}
	return inserts
}

// Helper methods

func (g *Generator) randomText(minLen, maxLen int) string {
	length := minLen + g.rng.Intn(maxLen-minLen+1)
	words := []string{"the", "of", "and", "to", "a", "in", "that", "is", "was", "for", "it", "with", "as", "his", "on", "be", "at", "by", "have", "from"}

	var result []string
	totalLen := 0
	for totalLen < length {
		word := words[g.rng.Intn(len(words))]
		result = append(result, word)
		totalLen += len(word) + 1
	}

	text := strings.Join(result, " ")
	if len(text) > length {
		text = text[:length]
	}
	return text
}

func (g *Generator) randomPhone(nationKey int) string {
	countryCode := nationKey + 10
	return fmt.Sprintf("%02d-%03d-%03d-%04d",
		countryCode,
		g.rng.Intn(900)+100,
		g.rng.Intn(900)+100,
		g.rng.Intn(9000)+1000)
}

func (g *Generator) randomDecimal(min, max float64) float64 {
	return min + g.rng.Float64()*(max-min)
}

func (g *Generator) randomPartName() string {
	syllables := []string{"al", "an", "ar", "be", "ca", "de", "el", "en", "es", "et", "ge", "in", "is", "it", "le", "ly", "ma", "me", "ne", "nu", "on", "or", "ou", "pe", "ra", "re", "ri", "ro", "se", "st", "te", "ti", "to", "un", "ur", "us"}

	numSyllables := 2 + g.rng.Intn(3)
	var parts []string
	for i := 0; i < numSyllables; i++ {
		parts = append(parts, syllables[g.rng.Intn(len(syllables))])
	}
	return strings.Join(parts, "")
}

// GenerateLineitem generates a single lineitem record
func (g *Generator) GenerateLineitem(orderKey, lineNumber, maxPartKey, maxSuppKey int) string {
	partKey := g.rng.Intn(maxPartKey) + 1
	suppKey := g.rng.Intn(maxSuppKey) + 1
	quantity := g.randomDecimal(1, 50)
	extendedPrice := quantity * g.randomDecimal(900, 1100)
	discount := g.randomDecimal(0, 0.10)
	tax := g.randomDecimal(0, 0.08)

	// Return flag and line status
	returnFlag := "N"
	lineStatus := "O"
	if g.rng.Float64() < 0.1 {
		returnFlag = "R"
	}
	if g.rng.Float64() < 0.5 {
		lineStatus = "F"
	}

	// Generate dates (ship date within 122 days of order date)
	baseDate := time.Date(1992, 1, 1, 0, 0, 0, 0, time.UTC)
	shipOffset := g.rng.Intn(2500) // days from base
	shipDate := baseDate.AddDate(0, 0, shipOffset)
	commitDate := shipDate.AddDate(0, 0, g.rng.Intn(30))
	receiptDate := shipDate.AddDate(0, 0, g.rng.Intn(7)+1)

	shipInstruct := []string{"DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"}[g.rng.Intn(4)]
	shipMode := []string{"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"}[g.rng.Intn(7)]
	comment := g.randomText(10, 43)

	return fmt.Sprintf("INSERT INTO lineitem VALUES (%d, %d, %d, %d, %.2f, %.2f, %.2f, %.2f, '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');",
		orderKey, partKey, suppKey, lineNumber, quantity, extendedPrice, discount, tax,
		returnFlag, lineStatus, shipDate.Format("2006-01-02"), commitDate.Format("2006-01-02"),
		receiptDate.Format("2006-01-02"), shipInstruct, shipMode, comment)
}

// GeneratePartsupp generates a single partsupp record
func (g *Generator) GeneratePartsupp(partKey, suppKey int) string {
	availQty := g.rng.Intn(9999) + 1
	supplyCost := g.randomDecimal(1.00, 1000.00)
	comment := g.randomText(49, 198)

	return fmt.Sprintf("INSERT INTO partsupp VALUES (%d, %d, %d, %.2f, '%s');",
		partKey, suppKey, availQty, supplyCost, comment)
}
