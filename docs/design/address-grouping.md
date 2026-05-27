# Address Grouping

## Overview

This document describes the design for a new address grouping feature in Apache Artemis. Address groups allow grouping multiple addresses together to share a common memory limit, providing more granular control over memory allocation and preventing resource exhaustion in multi-tenant or high-cardinality address scenarios.

## Motivation

Currently, Apache Artemis manages memory limits at the individual address level through `AddressSettings.maxSizeBytes` and `AddressSettings.maxSizeMessages`. While this provides fine-grained control, it has several limitations:

1. **Multi-tenant deployments**: In scenarios where each tenant has multiple addresses (e.g., `tenant1.orders`, `tenant1.payments`, `tenant2.orders`), there's no way to enforce a memory limit per tenant across all their addresses.

2. **High-cardinality addresses**: When using dynamic address creation with patterns (e.g., time-based addresses like `metrics.2026.05.27`, user-specific addresses), managing individual limits becomes impractical.

3. **Resource isolation**: Applications need to isolate resource consumption between different business domains or departments without creating separate broker instances.

4. **Fairness**: Without grouping, a single address can consume all available memory, starving other addresses even if they have the same configured limits.

## Goals

- Allow multiple addresses to be grouped into a single address group
- Each address can belong to at most one address group
- All addresses in a group share a common memory limit
- Memory accounting is done at the group level, not individual address level
- Configuration should be flexible and support pattern matching for address assignment
- Minimal performance impact on message routing and memory management
- Support for dynamic address creation with automatic group assignment

## Design

### Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                       Apache Artemis Broker                      │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                 AddressSettings Repository                 │  │
│  │                                                            │  │
│  │  Pattern: tenant1.#  →  addressGroup: tenant1-group        │  │
│  │  Pattern: tenant2.#  →  addressGroup: tenant2-group        │  │
│  │  Pattern: metrics.#  →  addressGroup: metrics-group        │  │
│  └────────────────────────────────────────────────────────────┘  │
│                              │                                   │
│                              ▼                                   │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │                     Address Groups                         │  │
│  │                                                            │  │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌────────────┐  │  │
│  │  │ tenant1-group   │  │ tenant2-group   │  │metrics-grp │  │  │
│  │  │ Max: 10GB       │  │ Max: 5GB        │  │ Max: 5GB   │  │  │
│  │  │ Used: 3.2GB     │  │ Used: 1.8GB     │  │ Used: 4.9GB│  │  │
│  │  │ Policy: PAGE    │  │ Policy: PAGE    │  │ Policy:DROP│  │  │
│  │  └────────┬────────┘  └────────┬────────┘  └─────┬──────┘  │  │
│  └───────────┼────────────────────┼──────────────────┼────────┘  │
│              │                    │                  │           │
│     ┌────────┴────────┐  ┌────────┴────────┐  ┌──────┴──────┐    │
│     │ tenant1.orders  │  │ tenant2.orders  │  │metrics.     │    │
│     │ tenant1.payments│  │ tenant2.payments│  │  2026.05.27 │    │
│     │ tenant1.shipping│  │ tenant2.shipping│  │metrics.     │    │
│     │ tenant1.billing │  │ tenant2.billing │  │  2026.05.28 │    │
│     │                 │  │                 │  │stats.app1   │    │
│     └─────────────────┘  └─────────────────┘  └─────────────┘    │
│                                                                  │
└──────────────────────────────────────────────────────────────────┘
```

**Key Relationships:**
- `AddressSettings` defines which addresses belong to which group (via pattern matching)
- `AddressGroup` tracks memory usage across all its member addresses
- Each `Address` can belong to at most one group
- Addresses without a group use traditional per-address limits

### Core Components

#### 1. AddressGroup Class

A new `AddressGroup` entity that represents a group:

```java
package org.apache.activemq.artemis.core.server.group;

public class AddressGroup {
   
   private final SimpleString name;
   
   // Memory limits for this group
   private long maxSizeBytes = -1;  // -1 means unlimited
   private long maxSizeMessages = -1;  // -1 means unlimited
   
   // Current usage tracking
   private final AtomicLong currentSizeBytes = new AtomicLong(0);
   private final AtomicLong currentSizeMessages = new AtomicLong(0);
   
   // Policy when group is full
   private AddressFullMessagePolicy addressFullMessagePolicy;
   
   // Reject threshold (similar to address-level)
   private long maxSizeBytesRejectThreshold = -1;
   
   // All addresses that belong to this group
   private final Set<SimpleString> addresses = new ConcurrentHashSet<>();
   
   // Methods for memory accounting
   public boolean reserve(long bytes, long messages);
   public void release(long bytes, long messages);
   public boolean isFull();
   public long getAvailableBytes();
   public long getAvailableMessages();
}
```

#### 2. AddressGroupConfiguration

Configuration for defining groups:

```java
package org.apache.activemq.artemis.core.config.group;

public class AddressGroupConfiguration implements Serializable {
   
   private String name;
   private Long maxSizeBytes;
   private Long maxSizeMessages;
   private AddressFullMessagePolicy addressFullMessagePolicy;
   private Long maxSizeBytesRejectThreshold;
}
```

#### 3. Integration with AddressSettings

Add group reference to `AddressSettings`:

```java
// In AddressSettings class
private SimpleString addressGroup = null;

public SimpleString getAddressGroup() {
   return addressGroup;
}

public AddressSettings setAddressGroup(SimpleString addressGroup) {
   this.addressGroup = addressGroup;
   return this;
}
```

The group assignment flows from `AddressSettings` to `AddressInfo` when an address is created or updated.

#### 4. Integration with AddressInfo

Modify `AddressInfo` to reference its group:

```java
// In AddressInfo class
private SimpleString addressGroupName = null;

public SimpleString getAddressGroupName() {
   return addressGroupName;
}

public AddressInfo setAddressGroupName(SimpleString addressGroupName) {
   this.addressGroupName = addressGroupName;
   return this;
}
```

When an address is created, the system:
1. Looks up the matching `AddressSettings` pattern
2. Retrieves the `address-group` value from the settings
3. Assigns the group to the `AddressInfo`
4. Registers the address with the configured group

#### 5. Integration with PagingManager

The paging system needs to be aware of groups to enforce limits correctly:

```java
// In PagingStore or similar
public boolean checkMemory(ServerMessage message) {
   AddressGroup group = getAddressGroup();
   
   if (group != null) {
      // Check group-level limits
      if (!group.reserve(message.getMemoryEstimate(), 1)) {
         // Group is full, apply group policy
         return handleGroupFull(group, message);
      }
   } else {
      // Fall back to address-level limits (existing behavior)
      return checkAddressMemory(message);
   }
   
   return true;
}
```

### Configuration

#### XML Configuration (broker.xml)

Groups are defined separately from address-settings. Address assignment to groups is done exclusively through the `<address-group>` element in `address-settings`.

```xml
<configuration>
   <core>
      <!-- Step 1: Define address groups with their limits and policies -->
      <address-groups>
         <address-group name="tenant1-group">
            <max-size-bytes>10GB</max-size-bytes>
            <max-size-messages>1000000</max-size-messages>
            <address-full-policy>PAGE</address-full-policy>
            <max-size-bytes-reject-threshold>-1</max-size-bytes-reject-threshold>
         </address-group>
         
         <address-group name="tenant2-group">
            <max-size-bytes>5GB</max-size-bytes>
            <max-size-messages>500000</max-size-messages>
            <address-full-policy>PAGE</address-full-policy>
         </address-group>
         
         <address-group name="metrics-group">
            <max-size-bytes>5GB</max-size-bytes>
            <address-full-policy>DROP</address-full-policy>
         </address-group>
      </address-groups>
      
      <!-- Step 2: Assign addresses to groups via address-settings -->
      <address-settings>
         <!-- All tenant1 addresses go to tenant1-group -->
         <address-setting match="tenant1.#">
            <address-group>tenant1-group</address-group>
            <page-size-bytes>10485760</page-size-bytes>
         </address-setting>
         
         <!-- All tenant2 addresses go to tenant2-group -->
         <address-setting match="tenant2.#">
            <address-group>tenant2-group</address-group>
            <page-size-bytes>10485760</page-size-bytes>
         </address-setting>
         
         <!-- Metrics and stats addresses go to metrics-group -->
         <address-setting match="metrics.#">
            <address-group>metrics-group</address-group>
         </address-setting>
         
         <address-setting match="stats.#">
            <address-group>metrics-group</address-group>
         </address-setting>
         
         <!-- Addresses without group use traditional per-address limits -->
         <address-setting match="legacy.#">
            <max-size-bytes>1GB</max-size-bytes>
            <address-full-policy>PAGE</address-full-policy>
         </address-setting>
      </address-settings>
   </core>
</configuration>
```

**Key Points:**

1. **Group Definition**: Defines the group name, limits, and policy
2. **Address Assignment**: Done through `<address-group>` element in `address-settings`
3. **Pattern Matching**: Uses existing `address-settings` match patterns (supports `#` and `*` wildcards)
4. **One Group Per Address**: Each address can only be assigned to one group (enforced by single `<address-group>` element)
5. **Optional Grouping**: Addresses without `<address-group>` element continue to use traditional per-address limits

#### Broker Properties Configuration (broker.properties)

As an alternative to XML, address groups can be configured using broker properties:

```properties
# Define address groups
addressGroup.tenant1-group.maxSizeBytes=10737418240
addressGroup.tenant1-group.maxSizeMessages=1000000
addressGroup.tenant1-group.addressFullMessagePolicy=PAGE
addressGroup.tenant1-group.maxSizeBytesRejectThreshold=-1

addressGroup.tenant2-group.maxSizeBytes=5368709120
addressGroup.tenant2-group.maxSizeMessages=500000
addressGroup.tenant2-group.addressFullMessagePolicy=PAGE

addressGroup.metrics-group.maxSizeBytes=5368709120
addressGroup.metrics-group.addressFullMessagePolicy=DROP

# Assign addresses to groups via address settings
addressSettings.tenant1.#.addressGroup=tenant1-group
addressSettings.tenant1.#.pageSizeBytes=10485760

addressSettings.tenant2.#.addressGroup=tenant2-group
addressSettings.tenant2.#.pageSizeBytes=10485760

addressSettings.metrics.#.addressGroup=metrics-group

addressSettings.stats.#.addressGroup=metrics-group

# Addresses without group use traditional per-address limits
addressSettings.legacy.#.maxSizeBytes=1073741824
addressSettings.legacy.#.addressFullMessagePolicy=PAGE
```

**Property Format:**

- Group definition: `addressGroup.<group-name>.<property>=<value>`
- Address assignment: `addressSettings.<address-pattern>.<property>=<value>`

**Supported Group Properties:**

- `maxSizeBytes` - Maximum size in bytes (numeric value, use -1 for unlimited)
- `maxSizeMessages` - Maximum number of messages (numeric value, use -1 for unlimited)
- `addressFullMessagePolicy` - Policy when full (PAGE, DROP, FAIL, BLOCK)
- `maxSizeBytesRejectThreshold` - Reject threshold in bytes (numeric value, use -1 for disabled)

**Address Settings Properties:**

- `addressGroup` - Name of the group to assign addresses to
- All other standard `addressSettings` properties continue to work

#### Programmatic Configuration

```java
// Create address group
AddressGroupConfiguration config = new AddressGroupConfiguration()
   .setName("tenant1-group")
   .setMaxSizeBytes(10L * 1024 * 1024 * 1024)  // 10GB
   .setMaxSizeMessages(1_000_000L)
   .setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);

server.createAddressGroup(config);

// Assign addresses to group via AddressSettings
AddressSettings settings = new AddressSettings()
   .setAddressGroup(SimpleString.of("tenant1-group"))
   .setPageSizeBytes(10 * 1024 * 1024);

server.getAddressSettingsRepository().addMatch("tenant1.#", settings);
```

### Memory Accounting Flow

```
                        Message Arrives
                              │
                              ▼
                    ┌─────────────────────┐
                    │  Get AddressInfo    │
                    └─────────┬───────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │ Has Address Group?  │
                    └─────────┬───────────┘
                              │
                 ┌────────────┴────────────┐
                 │                         │
                YES                       NO
                 │                         │
                 ▼                         ▼
    ┌────────────────────────┐   ┌─────────────────────┐
    │  Check Group Limits    │   │ Check Address Limits│
    │  reserve(bytes, count) │   │  (existing behavior)│
    └────────────┬───────────┘   └─────────┬───────────┘
                 │                         │
        ┌────────┴────────┐                │
        │                 │                │
     SUCCESS           FULL                │
        │                 │                │
        ▼                 ▼                │
   ┌─────────┐    ┌──────────────┐         │
   │ Accept  │    │ Apply Group  │         │
   │ Message │    │ Full Policy  │         │
   └─────────┘    │ (PAGE/DROP/  │         │
                  │  BLOCK/FAIL) │         │
                  └──────────────┘         │
                                           │
                                           ▼
                                    ┌─────────────┐
                                    │   Accept/   │
                                    │   Reject    │
                                    └─────────────┘


                    Message Consumed/Expired
                              │
                              ▼
                    ┌─────────────────────┐
                    │  Get AddressInfo    │
                    └─────────┬───────────┘
                              │
                              ▼
                    ┌─────────────────────┐
                    │ Has Address Group?  │
                    └─────────┬───────────┘
                              │
                 ┌────────────┴────────────┐
                 │                         │
                YES                       NO
                 │                         │
                 ▼                         ▼
    ┌────────────────────────┐   ┌─────────────────────┐
    │ Group.release(bytes,   │   │ Address.release()   │
    │              count)    │   │ (existing behavior) │
    └────────────────────────┘   └─────────────────────┘
```

#### Message Arrival Steps

1. Message arrives at address
2. System checks if address belongs to a group
3. If group exists:
   - Check group memory limits
   - If within limits, reserve memory at group level
   - Track message in group
4. If no group or group check passes:
   - Fall back to existing address-level checks

#### Message Consumption/Expiry Steps

1. When message is acknowledged/expired
2. System identifies the group (if any)
3. Release memory from group accounting
4. Update group metrics

#### Group Full Behavior

Two possible solutions for handling group memory limits:

##### Solution 1: Individual Address Policies

When the group reaches its limit, each address applies its own configured `AddressFullMessagePolicy` from its `AddressSettings`.

**Pros:**
- Maximum flexibility - different addresses can have different behaviors
- Supports mixed use cases (e.g., critical addresses use BLOCK, non-critical use DROP)
- Leverages existing address-level configuration
- No configuration duplication

**Cons:**
- Less predictable behavior across the group
- More complex to understand and debug
- Potential inconsistency if addresses have conflicting policies
- Policy might not make sense at group level (e.g., one address pages while another blocks)

**Example:**
```xml
<address-group name="tenant1-group">
   <max-size-bytes>10GB</max-size-bytes>
   <!-- No group-level policy -->
</address-group>

<address-setting match="tenant1.critical.#">
   <address-group>tenant1-group</address-group>
   <address-full-policy>BLOCK</address-full-policy>
</address-setting>

<address-setting match="tenant1.logs.#">
   <address-group>tenant1-group</address-group>
   <address-full-policy>DROP</address-full-policy>
</address-setting>
```

When group is full:
- Messages to `tenant1.critical.orders` will BLOCK
- Messages to `tenant1.logs.app` will DROP

##### Solution 2: Unified Group Policy

When the group reaches its limit, all addresses in the group apply the group's configured `AddressFullMessagePolicy`.

**Pros:**
- Consistent, predictable behavior across all addresses
- Simpler mental model - one group, one policy
- Easier to reason about resource management
- Clear group-level semantics

**Cons:**
- Less flexibility - all addresses must behave the same way
- May require duplicate groups for different policy needs
- Overrides address-level settings when group is full

**Example:**
```xml
<address-group name="tenant1-group">
   <max-size-bytes>10GB</max-size-bytes>
   <address-full-policy>PAGE</address-full-policy>
</address-group>

<address-setting match="tenant1.#">
   <address-group>tenant1-group</address-group>
   <!-- Address-level policy ignored when group is full -->
   <address-full-policy>BLOCK</address-full-policy>
</address-setting>
```

When group is full:
- All addresses under `tenant1.#` will PAGE regardless of their individual settings

##### Recommended Approach: Solution 2 (Unified Group Policy)

**Rationale:**

1. **Group-level resource management**: Groups are about managing resources as a group, not individually
2. **Predictability**: Operators can reason about group behavior without inspecting each address
3. **Consistency with group concept**: A group represents a shared resource pool with shared policies
4. **Simplicity**: One limit, one policy, easier to configure and monitor

**Policy Types:**

- **PAGE**: Messages are paged to disk across all addresses in the group
- **DROP**: New messages to any address in the group are dropped (can be logged)
- **FAIL**: Message send fails with `AddressFullException` for any address
- **BLOCK**: Producers to any address in the group block until space becomes available

**Hybrid Approach (Future Enhancement):**

For scenarios requiring different policies, consider:
- Multiple groups with different policies
- Group priority levels (critical vs. non-critical groups)
- Per-address policy override flag (opt-out of group policy)

### Address-Level vs Group-Level Limits

When an address belongs to a group, there are two possible approaches for handling address-level memory limits (`maxSizeBytes` in `AddressSettings`):

#### Option 1: Dual Limits (Both Checked)

Address-level memory limits are enforced **in addition to** group-level limits. Both limits must be satisfied for a message to be accepted.

**Behavior:**
- Message is checked against both address limit AND group limit
- Whichever limit is reached first triggers the policy
- Address limit acts as a "sub-limit" within the group

**Pros:**
- Prevents a single address from consuming the entire group
- Provides fine-grained control per address within group boundaries
- Useful for mixed-priority addresses (e.g., critical address gets 2GB, non-critical gets 500MB within a 10GB group)
- Better resource fairness within the group

**Cons:**
- More complex to configure and understand
- Requires maintaining two sets of limits
- Can be confusing which limit is being hit
- Policy conflict: which policy applies when address vs group limit is hit?
- More runtime overhead (two checks per message)

**Example:**
```xml
<address-group name="tenant1-group">
   <max-size-bytes>10GB</max-size-bytes>
   <address-full-policy>PAGE</address-full-policy>
</address-group>

<address-setting match="tenant1.critical.#">
   <address-group>tenant1-group</address-group>
   <max-size-bytes>2GB</max-size-bytes>  <!-- Still enforced -->
   <address-full-policy>BLOCK</address-full-policy>  <!-- Used when address limit hit -->
</address-setting>

<address-setting match="tenant1.logs.#">
   <address-group>tenant1-group</address-group>
   <max-size-bytes>500MB</max-size-bytes>  <!-- Still enforced -->
   <address-full-policy>DROP</address-full-policy>  <!-- Used when address limit hit -->
</address-setting>
```

**Scenario:**
- Group total: 10GB shared across all tenant1 addresses
- `tenant1.critical.orders`: Can grow to 2GB max (BLOCK when hit)
- `tenant1.logs.app`: Can grow to 500MB max (DROP when hit)
- If group reaches 10GB total: All addresses PAGE
- If `tenant1.critical.orders` reaches 2GB (but group still has space): BLOCK
- If `tenant1.logs.app` reaches 500MB (but group still has space): DROP

#### Option 2: Group Override (Address Limits Ignored)

Address-level memory limits are **ignored** when an address belongs to a group. Only group limits apply.

**Behavior:**
- Group limit is the sole memory constraint
- Address-level `maxSizeBytes`/`maxSizeMessages` are not checked
- All memory management happens at group level

**Pros:**
- Simpler mental model - one source of truth for limits
- No configuration conflicts or confusion
- Clearer semantics: group membership means group-managed
- Less runtime overhead (single check)
- Easier to reason about memory distribution
- Avoids policy conflicts

**Cons:**
- Individual addresses can potentially consume entire group
- Less protection against runaway single address
- Reduces flexibility for per-address limits
- May require more groups to achieve fine-grained control

**Example:**
```xml
<address-group name="tenant1-group">
   <max-size-bytes>10GB</max-size-bytes>
   <address-full-policy>PAGE</address-full-policy>
</address-group>

<address-setting match="tenant1.#">
   <address-group>tenant1-group</address-group>
   <max-size-bytes>2GB</max-size-bytes>  <!-- IGNORED -->
   <address-full-policy>BLOCK</address-full-policy>  <!-- IGNORED when group limit hit -->
   <page-size-bytes>10485760</page-size-bytes>  <!-- Still applies -->
</address-setting>
```

**Scenario:**
- Group total: 10GB shared across all tenant1 addresses
- Any single address can grow up to 10GB if others are empty
- When group reaches 10GB total: All addresses PAGE
- Address-level settings (except group assignment) are ignored for memory limiting

#### Recommended Approach: Option 2 (Group Override)

**Rationale:**

1. **Simplicity**: One limit system, no confusion about which limit applies
2. **Clear ownership**: Group membership means group-managed resources
3. **Performance**: Single memory check per message, less overhead
4. **Consistency**: Aligns with "group is a shared resource pool" concept
5. **Predictability**: Operators know group limit is the authority

**Address Fairness Concerns:**

To prevent a single address from consuming the entire group:

1. **Multiple groups approach**: Create separate groups with different limits
   ```xml
   <address-group name="tenant1-critical">
      <max-size-bytes>7GB</max-size-bytes>
   </address-group>
   
   <address-group name="tenant1-noncritical">
      <max-size-bytes>3GB</max-size-bytes>
   </address-group>
   ```

2. **Per-address soft limits (monitoring)**: Use metrics/alerts to detect imbalanced consumption
   - Alert when single address > 50% of group
   - Management API to query per-address usage within group

3. **Future enhancement - weighted allocation**: 
   ```xml
   <address-setting match="tenant1.critical.#">
      <address-group>tenant1-group</address-group>
      <group-weight>70</group-weight>  <!-- Gets 70% of group -->
   </address-setting>
   ```

**Configuration Inheritance:**

With Option 2, address settings still apply for non-memory configurations:
- ✅ `page-size-bytes` - still used
- ✅ `redelivery-delay` - still used
- ✅ `dead-letter-address` - still used
- ❌ `max-size-bytes` - ignored (group limit applies)
- ❌ `address-full-policy` - ignored when group limit hit (group policy applies)

**Migration Path:**

For systems with existing address-level limits migrating to groups:
1. Group limits should be set to sum of current address limits
2. Remove address-level limits to avoid confusion (they're ignored anyway)
3. Document that group assignment overrides address memory limits

### Address Assignment via AddressSettings

Address-to-group assignment is done exclusively through `address-settings` using the `<address-group>` element.

```
Address Creation Flow:

  New Address: "tenant1.critical.orders"
           │
           ▼
  ┌─────────────────────────┐
  │ Lookup AddressSettings  │
  │ Pattern Matching        │
  └────────────┬────────────┘
               │
               ▼
  ┌─────────────────────────────────────────────┐
  │  Match Patterns (Most Specific First):      │
  │                                             │
  │  1. tenant1.critical.orders (exact) ✗       │
  │  2. tenant1.critical.# ✓                    │
  │     → addressGroup: tenant1-critical-group  │
  │  3. tenant1.# ✓ (less specific, ignored)    │
  └────────────┬────────────────────────────────┘
               │
               ▼
  ┌─────────────────────────┐
  │ Apply Settings:         │
  │ - addressGroup =        │
  │   tenant1-critical-grp  │
  │ - other settings...     │
  └────────────┬────────────┘
               │
               ▼
  ┌─────────────────────────┐
  │ Create AddressInfo      │
  │ with group assignment   │
  └────────────┬────────────┘
               │
               ▼
  ┌─────────────────────────┐
  │ Register address in     │
  │ tenant1-critical-group  │
  └─────────────────────────┘
```

**Pattern Matching:**

Uses the existing `address-settings` pattern matching mechanism:

1. **Exact match**: `myaddress` matches only "myaddress"
2. **Wildcard `*`**: Matches a single word, e.g., `tenant1.*.orders` matches `tenant1.prod.orders`
3. **Multi-level wildcard `#`**: Matches zero or more words, e.g., `tenant1.#` matches `tenant1.orders`, `tenant1.prod.orders`, etc.

**Pattern Precedence:**

Follows standard `address-settings` precedence rules:
1. Exact match (most specific)
2. Longest wildcard match
3. First defined pattern (if equal length)

**Constraint Enforcement:**

Each address can only belong to one group. If multiple `address-settings` patterns match an address and specify different groups, the most specific match wins (following standard `address-settings` merge behavior).

**Example:**
```xml
<address-setting match="tenant1.#">
   <address-group>tenant1-group</address-group>
</address-setting>

<address-setting match="tenant1.critical.#">
   <address-group>tenant1-critical-group</address-group>  <!-- More specific, wins -->
</address-setting>
```

Address `tenant1.critical.orders` will be assigned to `tenant1-critical-group` (more specific match).

### Metrics and Management

#### JMX/Management API

New management operations on `ArtemisServerControl`:

```java
// List all groups
String[] listAddressGroups();

// Get group details
String getAddressGroupInfo(String groupName) throws Exception;

// Create/update group
void createAddressGroup(String config) throws Exception;

// Delete group
void deleteAddressGroup(String name, boolean force) throws Exception;
```

New MBean interface `AddressGroupControl`:

```java
public interface AddressGroupControl {
   String getName();
   long getMaxSizeBytes();
   long getMaxSizeMessages();
   long getCurrentSizeBytes();
   long getCurrentSizeMessages();
   int getAddressCount();
   String[] getAddresses();
   String getAddressFullPolicy();
   boolean isFull();
   double getUsagePercentage();
}
```

#### Metrics

New metrics exposed:

- `artemis.address.group.size.bytes{group="name"}` - Current group size in bytes
- `artemis.address.group.size.messages{group="name"}` - Current message count
- `artemis.address.group.usage.percentage{group="name"}` - Usage percentage
- `artemis.address.group.address.count{group="name"}` - Number of addresses in group
- `artemis.address.group.full{group="name"}` - Boolean indicating if group is full

### Persistence

Group configuration is persisted in broker configuration file. Group membership for addresses is stored with `AddressInfo`:

```java
// JSON representation in AddressInfo
{
   "id": 123,
   "name": "tenant1.orders",
   "routingTypes": [0],
   "addressGroup": "tenant1-group",  // New field
   ...
}
```

### Migration and Compatibility

#### Backward Compatibility

- Existing address-settings without group assignment continue to work unchanged
- Per-address memory limits (maxSizeBytes) remain functional for non-grouped addresses
- No breaking changes to existing APIs

#### Migration Path

For systems migrating to groups:

1. Define groups in `<address-groups>` configuration
2. Add `<address-group>` element to relevant `<address-setting>` entries
3. Addresses without `<address-group>` element use existing per-address behavior
4. Gradual migration: can mix grouped and non-grouped addresses

### Edge Cases and Error Handling

#### Group Deletion

- **Non-empty group**: By default, fails unless `force=true`
- **Force deletion**: Removes group, addresses revert to individual limits
- **In-flight messages**: Counted against address limits after group removal

#### Address Reassignment

- Moving address from one group to another:
  - Current memory usage released from old group
  - Current memory usage reserved in new group
  - Fails if new group cannot accommodate current usage

#### Dynamic Address Creation

- New addresses automatically assigned to group based on `address-settings` pattern matching
- Group assignment happens at address creation time
- Assignment is determined by the most specific `address-setting` match containing an `<address-group>` element
- Assignment is sticky (doesn't change if `address-settings` patterns are modified later)

#### Group Limit Changes

- Reducing group limit below current usage:
  - Change accepted but no new messages until usage drops
  - Existing messages remain (not forcefully deleted)
  - Triggers appropriate policy (PAGE, BLOCK, etc.)

#### Overlapping Address-Settings Patterns

- If multiple `address-settings` match an address with different `<address-group>` values:
  - Most specific pattern wins (standard `address-settings` precedence)
  - Settings are merged following standard rules
  - Group assignment comes from the winning match
  - No warnings needed (follows existing `address-settings` behavior)

#### Group Not Found

- If `address-settings` references a non-existent group:
  - Log warning at address creation time
  - Address falls back to traditional per-address limits
  - Or fail address creation (configurable behavior)

## Implementation Phases

### Phase 1: Core Infrastructure
- Implement `AddressGroup` and `AddressGroupConfiguration`
- Add `address-group` field to `AddressSettings`
- Add group reference to `AddressInfo`
- Configuration parsing for groups in broker.xml
- Add group management operations to `ArtemisServer`

### Phase 2: Address-Settings Integration
- Modify address creation logic to read group from `AddressSettings`
- Implement group assignment when address is created
- Handle group lookup via `AddressSettings` hierarchy

### Phase 3: Memory Accounting
- Integrate group checks into message routing
- Modify `PagingManager` to work with groups
- Implement reserve/release mechanisms
- Handle group full policies

### Phase 4: Management and Monitoring
- Add JMX management operations
- Implement `AddressGroupControl` MBean
- Add metrics export
- Runtime group limits modification

## Performance Considerations

### Memory Overhead

- Each group: ~200 bytes for object overhead + data structures
- Per-address group reference: 8 bytes (pointer)
- Address-to-group map: O(number of addresses) memory

Expected overhead for 1000 groups with 10,000 addresses: ~1-2 MB

### CPU Overhead

- Address-to-group lookup: O(1) via HashMap
- Memory reservation: Atomic operations, minimal contention
- Pattern matching: Only at address creation time

### Concurrency

- All group operations use lock-free data structures (AtomicLong, ConcurrentHashMap)
- No global locks for memory accounting
- Group-level contention only for addresses in same group

## Security Considerations

- Group creation/deletion requires admin privileges
- RBAC integration: permissions can be scoped to groups
- Group isolation prevents resource exhaustion attacks across tenants

## Testing Strategy

### Unit Tests
- Group creation, deletion, updates
- Memory accounting (reserve, release)
- Pattern matching logic
- Edge cases (group full, limit changes)

### Integration Tests
- Multi-address scenarios with group limits
- Paging behavior with groups
- Dynamic address creation with group assignment
- Management operations

### Performance Tests
- Throughput impact with groups
- Memory overhead measurement
- Concurrent access to grouped addresses
- Large-scale tests (1000+ groups, 100k+ addresses)

## Documentation Requirements

- User manual section on address grouping
- Configuration reference for group settings
- Migration guide from address-level to group-level limits
- Examples for common use cases (multi-tenancy, time-series data)
- JMX/management API documentation

## Future Enhancements

- **Cross-broker groups**: Group coordination in clustered environments
- **Group hierarchy**: Nested groups for multi-level resource management
- **Auto-scaling groups**: Dynamic limit adjustment based on load
- **Group templates**: Reusable group configurations
- **Resource types**: Extend beyond memory to include connections, threads

## References

- Existing `AddressSettings` implementation: `artemis-server/src/main/java/org/apache/activemq/artemis/core/settings/impl/AddressSettings.java`
- `AddressInfo` implementation: `artemis-server/src/main/java/org/apache/activemq/artemis/core/server/impl/AddressInfo.java`
- Paging system: `artemis-server/src/main/java/org/apache/activemq/artemis/core/paging/`
- Similar pattern: Hierarchical address settings with pattern matching

## Appendix: Use Cases

### Use Case 1: Multi-Tenant SaaS Application

**Scenario**: SaaS platform with 100 tenants, each tenant has multiple queues for different services.

```
┌─────────────────────────────────────────────────────────────────┐
│                    Multi-Tenant Deployment                      │
│                                                                 │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────────┐ │
│  │  Tenant 1 Group  │  │  Tenant 2 Group  │  │ Tenant 3 Group │ │
│  │  Max: 5GB        │  │  Max: 5GB        │  │  Max: 5GB      │ │
│  │  Used: 3.1GB     │  │  Used: 4.8GB     │  │  Used: 1.2GB   │ │
│  │  Policy: PAGE    │  │  Policy: PAGE    │  │  Policy: PAGE  │ │
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬───────┘ │
│           │                     │                     │         │
│  ┌────────┴─────────┐  ┌────────┴─────────┐  ┌────────┴───────┐ │
│  │ tenant1.orders   │  │ tenant2.orders   │  │tenant3.orders  │ │
│  │ tenant1.payments │  │ tenant2.payments │  │tenant3.payment │ │
│  │ tenant1.shipping │  │ tenant2.shipping │  │tenant3.shipping│ │
│  │ tenant1.billing  │  │ tenant2.billing  │  │tenant3.billing │ │
│  └──────────────────┘  └──────────────────┘  └────────────────┘ │
│                                                                 │
│  Isolation: Each tenant limited to 5GB regardless of            │
│             address count or message distribution               │
└─────────────────────────────────────────────────────────────────┘
```

**Solution**:
```xml
<address-group name="tenant-{id}-group">
   <max-size-bytes>5GB</max-size-bytes>
   <address-full-policy>PAGE</address-full-policy>
</address-group>

<address-setting match="tenant.{id}.#">
   <address-group>tenant-{id}-group</address-group>
</address-setting>
```

**Benefit**: Each tenant gets isolated 5GB memory allocation, preventing noisy neighbor issues.

### Use Case 2: Time-Series Metrics Collection

**Scenario**: Application creates daily addresses for metrics (e.g., `metrics.2026.05.27`).

```
┌──────────────────────────────────────────────────────────────┐
│              Time-Series Metrics Collection                  │
│                                                              │
│  ┌────────────────────────────────────────────────────────┐  │
│  │              Metrics Group                             │  │
│  │              Max: 20GB                                 │  │
│  │              Used: 18.5GB                              │  │
│  │              Policy: DROP (oldest data dropped)        │  │
│  └──────────────────────┬─────────────────────────────────┘  │
│                         │                                    │
│         ┌───────────────┼───────────────┬─────────────┐      │
│         │               │               │             │      │
│  ┌──────▼──────┐ ┌──────▼──────┐ ┌──────▼──────┐ ┌───▼──┐    │
│  │metrics.     │ │metrics.     │ │metrics.     │ │...   │    │
│  │2026.05.24   │ │2026.05.25   │ │2026.05.26   │ │      │    │
│  │0.8GB        │ │3.2GB        │ │7.1GB        │ │      │    │
│  └─────────────┘ └─────────────┘ └─────────────┘ └──────┘    │
│                                                              │
│  As new daily addresses are created:                         │
│  - All share the 20GB limit                                  │
│  - When full, new messages are DROPped                       │
│  - Auto-cleanup via address TTL removes old addresses        │
└───────────────────────────────────────────────────────────────┘
```

**Solution**:
```xml
<address-group name="metrics-group">
   <max-size-bytes>20GB</max-size-bytes>
   <max-size-messages>10000000</max-size-messages>
   <address-full-policy>DROP</address-full-policy>
</address-group>

<address-setting match="metrics.#">
   <address-group>metrics-group</address-group>
</address-setting>
```

**Benefit**: All time-series addresses share 20GB limit, old data auto-deleted via DROP policy.

### Use Case 3: Department-Level Resource Allocation

**Scenario**: Enterprise with departments (Sales, Engineering, Support), each department has multiple business queues.

```
┌──────────────────────────────────────────────────────────────────┐
│                  Enterprise Resource Allocation                  │
│                                                                  │
│  ┌─────────────────┐  ┌──────────────────┐  ┌─────────────────┐  │
│  │  Sales Group    │  │ Engineering Grp  │  │  Support Group  │  │
│  │  Max: 15GB      │  │  Max: 25GB       │  │  Max: 10GB      │  │
│  │  Used: 8.2GB    │  │  Used: 19.3GB    │  │  Used: 3.1GB    │  │
│  │  Policy: PAGE   │  │  Policy: PAGE    │  │  Policy: BLOCK  │  │
│  └────────┬────────┘  └────────┬─────────┘  └────────┬────────┘  │
│           │                    │                     │           │
│    ┌──────┴──────┐      ┌──────┴──────┐       ┌──────┴──────┐    │
│    │ sales.      │      │ engineering.│       │ support.    │    │
│    │ - leads     │      │ - builds    │       │ - tickets   │    │
│    │ - orders    │      │ - deploys   │       │ - escalate  │    │
│    │ - quotes    │      │ - tests     │       │ - chat      │    │
│    │ - reports   │      │ dev.        │       └─────────────┘    │
│    └─────────────┘      │ - sandbox   │                          │
│                         │ - feature-x │                          │
│                         └─────────────┘                          │
│                                                                  │
│  Budget-Based Allocation:                                        │
│  - Sales: 15GB (standard allocation)                             │
│  - Engineering: 25GB (larger due to CI/CD needs)                 │
│  - Support: 10GB (lower traffic)                                 │
└──────────────────────────────────────────────────────────────────┘
```

**Solution**:
```xml
<address-group name="sales-group">
   <max-size-bytes>15GB</max-size-bytes>
</address-group>

<address-setting match="sales.#">
   <address-group>sales-group</address-group>
</address-setting>

<address-group name="engineering-group">
   <max-size-bytes>25GB</max-size-bytes>
</address-group>

<address-setting match="engineering.#">
   <address-group>engineering-group</address-group>
</address-setting>

<address-setting match="dev.#">
   <address-group>engineering-group</address-group>
</address-setting>

<address-group name="support-group">
   <max-size-bytes>10GB</max-size-bytes>
   <address-full-policy>BLOCK</address-full-policy>
</address-group>

<address-setting match="support.#">
   <address-group>support-group</address-group>
</address-setting>
```

**Benefit**: Department-level resource quotas without per-queue management overhead.

---

**Document Version**: 1.0  
**Last Updated**: 2026-05-27  
**Author**: Apache Artemis Development Team
